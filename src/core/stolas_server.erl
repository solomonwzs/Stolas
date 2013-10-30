-module(stolas_server).
-behaviour(gen_server).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-define(PING_INTERVAL, 1500).

-define(id(Name, Type), list_to_atom(lists:concat([Name, ":", Type]))).
-define(sub_workspace(Workspace, X), filename:join(Workspace, X)).
-define(broadcast_workers_msg(Task, ThreadNum, Msg),
        lists:foreach(fun(X)->
                              gen_server:cast(?id(Task, X), Msg)
                      end, lists:seq(1, ThreadNum))).
-define(ping_tref(Nodes),
        timer:apply_interval(
          ?PING_INTERVAL,
          lists, foreach, [fun(Node)->
                                   net_adm:ping(Node)
                           end, Nodes])).
-define(get_valid_nodes(ConfNodes),
        sets:to_list(sets:intersection(
                       sets:from_list(ConfNodes),
                       sets:from_list(nodes())
                      ))).
-define(master_spec(MasterId, ThreadNum, Mod, Workspace, Task, ConfNodes), {
          MasterId,
          {stolas_server, start_link,
           [MasterId, [{role, master}, {thread_num, ThreadNum},
                       {mod, Mod}, {task, Task},
                       {workspace, ?sub_workspace(Workspace, "master")},
                       {valid_nodes, ?get_valid_nodes(ConfNodes)}]]},
          permanent, 5000, worker, [stolas_server]
         }).
-define(worker_specs(MasterId, ThreadNum, Mod, Workspace, Task),
        lists:map(fun(X)->
                          {?id(Task, X),
                           {stolas_server, start_link,
                            [?id(Task, X), [{role, worker}, {mod, Mod},
                                            {master, MasterId},
                                            {workspace, 
                                             ?sub_workspace(
                                                Workspace,
                                                integer_to_list(X))}]]},
                           permanent, 5000, worker, [stolas_server]
                          }
                  end, lists:seq(1, ThreadNum))).
-define(start_task(Task, ChildSpecs),
        supervisor:start_child(
          stolas_sup,
          {Task,
           {stolas_simple_sup, start_link, [Task, ChildSpecs]},
           permanent, infinity, supervisor, [stolas_simple_sup]})).

-record(manager_state, {
          task_sets::tuple(),
          ping_tref::term(),
          is_master::true|false,
          config::list(tuple())
         }).
-record(worker_state, {
          mod::atom(),
          name::atom(),
          workspace::string(),
          master::atom()|{atom(), atom()}
         }).
-record(master_state, {
          task::atom(),
          mod::atom(),
          workspace::string(),
          thread_num::integer(),
          worker_results::tuple(),
          finish_tasks::integer()
         }).
-record(failure_msg, {
          task::atom(),
          role::atom(),
          reason::atom(),
          msg::term()
         }).

start_link(RegName, Opt)->
    case proplists:get_value(role, Opt) of
        master->
            Workspace=proplists:get_value(workspace, Opt),
            Mod=proplists:get_value(mod, Opt),
            ThreadNum=proplists:get_value(thread_num, Opt),
            Task=proplists:get_value(task, Opt),
            ValidNodes=proplists:get_value(valid_nodes, Opt),
            gen_server:start_link({local, RegName}, ?MODULE,
                                  [master, Mod, Workspace, ThreadNum, Task,
                                   ValidNodes],
                                  []);
        manager->
            Conf=proplists:get_value(config, Opt, []),
            gen_server:start_link({local, RegName}, ?MODULE, [manager, Conf],
                                  []);
        worker->
            Workspace=proplists:get_value(workspace, Opt),
            Mod=proplists:get_value(mod, Opt),
            Master=proplists:get_value(master, Opt),
            gen_server:start_link({local, RegName}, ?MODULE,
                                  [worker, Mod, Workspace, Master, RegName], [])
    end.

init([manager, Conf])->
    process_flag(trap_exit, true),
    {PingTref, IsMaster}=process_conf(Conf),
    {ok, #manager_state{
            task_sets=sets:new(),
            ping_tref=PingTref,
            is_master=IsMaster,
            config=Conf
           }};
init([master, Mod, Workspace, ThreadNum, Task, ValidNodes])->
    process_flag(trap_exit, true),
    {ok, #master_state{
            mod=Mod,
            workspace=Workspace,
            thread_num=ThreadNum,
            worker_results=dict:from_list([{Node, []}
                                           ||Node<-[node()|ValidNodes]]),
            task=Task,
            finish_tasks=0
           }};
init([worker, Mod, Workspace, Master, RegName])->
    process_flag(trap_exit, true),
    {ok, #worker_state{
            mod=Mod,
            name=RegName,
            workspace=Workspace,
            master=Master
           }}.

handle_call({alloc, WorkerName}, {Pid, _}, State=#master_state{
                                                    mod=Mod,
                                                    task=Task
                                                   })->
    Node=node(Pid),
    try
        Task=apply(Mod, alloc, [Node]),
        {reply, Task, State}
    catch
        T:R->
            error_logger:info_msg("Task:~p alloc task fail, node:~p, worker:~p",
                                  [Task, Node, WorkerName]),
            gen_server:cast(stolas_manager,
                            {close_task, Task,
                             #failure_msg{
                                task=Task,
                                role=master,
                                reason={T, R},
                                msg="unexpected error"
                               }}),
            {reply, error, State}
    end;

handle_call(get_config, _From, State=#manager_state{
                                        config=Conf
                                       })->
    {reply, {ok, Conf}, State};
handle_call(reload_config, _From, State=#manager_state{
                                           task_sets=TaskSets
                                          })->
    case sets:size(TaskSets) of
        0->
            NewConf=stolas_utils:get_value(default),
            cancel_conf(State),
            {NewPingTref}=process_conf(NewConf),
            NewState=State#manager_state{
                       config=NewConf,
                       ping_tref=NewPingTref
                      },
            {reply, {ok, NewState}, NewState};
        _->{reply, {error, "task list is not empty"}, State}
    end;
handle_call({new_task, Opt}, _From,
            State=#manager_state{
                     task_sets=TaskSets,
                     is_master=IsMaster,
                     config=Conf
                    }) when IsMaster=:=true->
    Task=proplists:get_value(task, Opt),
    case sets:is_element(Task, TaskSets) of
        true->
            {reply, {error, "already existed"}, State};
        false->
            Mod=proplists:get_value(mod, Opt),
            Workspace=proplists:get_value(workspace, Opt),
            ThreadNum=proplists:get_value(thread_num, Opt),
            MasterId=?id(Task, master),
            ConfNodes=proplists:get_value(Conf, nodes, []),
            MasterSpec=?master_spec(MasterId, ThreadNum, Mod, Workspace, Task,
                                    ConfNodes),
            WorkerSpecs=?worker_specs(MasterId, ThreadNum, Mod, Workspace, Task),
            Res=?start_task(Task, [MasterSpec|WorkerSpecs]),
            case Res of
                {ok, _}->
                    NewTaskSets=sets:add_element(Task, TaskSets),
                    InitArgs=proplists:get_value(init_args, Opt),
                    gen_server:cast(MasterId, {init, InitArgs}),
                    {reply, Res, State#manager_state{task_sets=NewTaskSets}};
                _->{reply, {error, Res}, State}
            end
    end;
handle_call(_Msg, _From, State)->
    {reply, {error, "error message"}, State}.

handle_cast(start_task, State=#worker_state{
                                 workspace=Workspace
                                })->
    file:make_dir(Workspace),
    gen_server:cast(self(), get_task),
    {noreply, State};
handle_cast(get_task, State=#worker_state{
                               mod=Mod,
                               workspace=Workspace,
                               master=Master,
                               name=WorkerName
                              })->
    Name={node(), WorkerName},
    case gen_server:call(Master, alloc) of
        none->gen_server:cast(Master, {reduce, {ok, WorkerName}});
        {ok, TaskArgs}->
            try
                case apply(Mod, map, [Workspace, TaskArgs]) of
                    {ok, Result}->
                        gen_server:cast(Master, {map_ok, Name, Result}),
                        gen_server:cast(self(), get_task);
                    {error, Reason}->
                        gen_server:cast(Master, {map_error, Name, Reason})
                end
            catch
                T:R->gen_server:cast(Master, {map_error, Name, {T, R}})
            end
    end,
    {noreply, State};

handle_cast({init, InitArgs}, State=#master_state{
                                       task=Task,
                                       thread_num=ThreadNum,
                                       workspace=Workspace,
                                       mod=Mod
                                      })->
    try
        file:make_dir(Workspace),
        case apply(Mod, init, [Workspace, InitArgs]) of
            ok->
                ?broadcast_workers_msg(Task, ThreadNum, start_task);
            {error, Reason}->
                gen_server:cast(stolas_manager,
                                {close_task, Task,
                                 #failure_msg{
                                    task=Task,
                                    role=master,
                                    reason=Reason,
                                    msg="Task initialization failed"
                                   }})
        end
    catch
        T:R->gen_server:cast(stolas_manager,
                             {close_task, Task,
                              #failure_msg{
                                 task=Task,
                                 role=master,
                                 reason={T, R},
                                 msg="unexpected error"
                                }})
    end,
    {noreply, State};
handle_cast({reduce, {ok, Name={_Node, _WorkerName}}},
            State=#master_state{
                     finish_tasks=FinishTasks,
                     mod=Mod,
                     workspace=Workspace,
                     thread_num=ThreadNum,
                     task=Task
                    })->
    error_logger:info_msg("Task:~p, Worker:~p map finish", [Task, Name]),
    if
        FinishTasks+1=:=ThreadNum->
            try
                case apply(Mod, reduce, [Workspace]) of
                    ok->
                        gen_server:cast(stolas_manager,
                                        {close_task, Task, normal});
                    {error, Reason}->
                        gen_server:cast(stolas_manager,
                                        {close_task, Task,
                                         #failure_msg{
                                            task=Task,
                                            role=master,
                                            reason=Reason,
                                            msg="Task reduce failed"
                                           }})
                end
            catch
                T:R->gen_server:cast(stolas_manager,
                                     {close_task, Task,
                                      #failure_msg{
                                         task=Task,
                                         role=master,
                                         reason={T, R},
                                         msg="unexpected error"
                                        }})
            end;
        true->ok
    end,
    {noreply, State#master_state{finish_tasks=FinishTasks+1}};
handle_cast({map_error, Name, Reason}, State=#master_state{
                                                      task=Task
                                                     })->
    error_logger:info_msg("Task:~p, Worker:~p map failed", [Task, Name]),
    gen_server:cast(stolas_manager, {close_task, Task,
                                     #failure_msg{
                                        task=Task,
                                        role=worker,
                                        reason=Reason,
                                        msg="Task map failed"
                                       }}),
    {noreply, State};

handle_cast({close_task, Task, Res}, State=#manager_state{
                                              task_sets=TaskSets
                                             })->
    case sets:is_element(Task, TaskSets) of
        true->
            supervisor:terminate_child(stolas_sup, Task),
            supervisor:delete_child(stolas_sup, Task),
            if
                Res=:=normal->
                    error_logger:info_msg("Task:~p completed", [Task]);
                Res=:=force->
                    error_logger:info_msg("Task:~p was closed forcibly",
                                          [Task]);
                is_record(Res, failure_msg)->
                    #failure_msg{
                       role=Role,
                       reason=Reason,
                       msg=Msg
                      }=Res,
                    error_logger:error_msg(
                      "Task:~p failed, role:~p, reason:~p, msg:~p",
                      [Task, Role, Reason, Msg])
            end,
            {noreply, State#manager_state{
                        task_sets=sets:del_element(Task, TaskSets)}};
        false->
            {noreply, State}
    end;

handle_cast(_Msg, State)->
    {noreply, State}.

handle_info(_Msg, State)->
    {noreply, State}.

code_change(_Vsn, State, _Extra)->
    {ok, State}.

terminate(_Reason, _State=#manager_state{
                             ping_tref=PingTref
                            })->
    timer:cancel(PingTref),
    ok;
terminate(_Reason, _State)->
    ok.

process_conf(Conf)->
    Nodes=proplists:get_value(nodes, Conf, [node()]),
    {ok, PingTref}=?ping_tref(Nodes),

    case proplists:get_value(ssh_files_transport, Conf, false) of
        true->ssh:start();
        _->ssh:stop()
    end,
    case proplists:get_value(readable_file_log, Conf) of
        LogPath when is_list(LogPath)->
            error_logger:add_report_handler(stolas_log_handler, [LogPath]);
        _->ok
    end,
    IsMaster=proplists:get_value(master, Conf)=:=node(),
                 
    {PingTref, IsMaster}.

cancel_conf(#manager_state{
               ping_tref=PingTref
              })->
    timer:cancel_conf(PingTref),
    error_logger:delete_report_handler(stolas_log_handler),
    ok.
