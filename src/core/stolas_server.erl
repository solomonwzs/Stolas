-module(stolas_server).
-behaviour(gen_server).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-define(PING_INTERVAL, 1500).

-define(id(Name, Type), list_to_atom(lists:concat([Name, ":", Type]))).
-define(sub_workspace(Workspace, X), filename:join(Workspace, X)).
-define(broadcast_map_msg(Task, ThreadNum),
        lists:foreach(fun(X)->
                              gen_server:cast(?id(Task, X), map)
                      end, lists:seq(1, ThreadNum))).

-record(manager_state, {
          task_sets::tuple(),
          role::master|sub,
          ping_tref::term(),
          config::list(tuple())
         }).
-record(worker_state, {
          mod::atom(),
          name::atom(),
          workspace::string(),
          master::atom()
         }).
-record(master_state, {
          task::atom(),
          mod::atom(),
          workspace::string(),
          thread_num::integer(),
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
            gen_server:start_link({local, RegName}, ?MODULE,
                                  [master, Mod, Workspace, ThreadNum, Task],
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
    Nodes=proplists:get_value(nodes, Conf, [node()]),
    {ok, PingTref}=timer:apply_interval(
                     ?PING_INTERVAL, erlang, spawn,
                     [lists, foreach, [fun(Node)->
                                               net_adm:ping(Node)
                                       end, Nodes]]),
    {ok, #manager_state{
            task_sets=sets:new(),
            ping_tref=PingTref,
            config=Conf
           }};
init([master, Mod, Workspace, ThreadNum, Task])->
    process_flag(trap_exit, true),
    {ok, #master_state{
            mod=Mod,
            workspace=Workspace,
            thread_num=ThreadNum,
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

handle_call(get_config, _From, State)
  when is_record(State, manager_state)->
    {reply, {ok, State#manager_state.config}, State};
handle_call(reload_config, _From, State)
  when is_record(State, manager_state)->
    case sets:size(State#manager_state.task_sets) of
        0->
            NewState=State#manager_state{
                       config=stolas_utils:get_value(default)},
            {reply, {ok, NewState}, NewState};
        _->{reply, {error, "task list is not empty"}, State}
    end;
handle_call({new_task, Opt}, _From, State)
  when is_record(State, manager_state)->
    TaskSets=State#manager_state.task_sets,
    Task=proplists:get_value(task, Opt),
    case sets:is_element(Task, TaskSets) of
        true->
            {reply, {error, "already existed"}, State};
        false->
            Mod=proplists:get_value(mod, Opt),
            Workspace=proplists:get_value(workspace, Opt),
            ThreadNum=proplists:get_value(thread_num, Opt),
            MasterId=?id(Task, master),
            MasterSpec={
              MasterId,
              {stolas_server, start_link,
               [MasterId, [{role, master}, {thread_num, ThreadNum},
                           {mod, Mod}, {workspace, Workspace}, {task, Task}]]},
              permanent,
              5000,
              worker,
              [stolas_server]
             },
            ChildSpecs=[{
              ?id(Task, X),
              {stolas_server, start_link,
               [?id(Task, X), [{role, worker}, {mod, Mod},
                               {master, MasterId},
                               {workspace, 
                                ?sub_workspace(Workspace,
                                               integer_to_list(X))}]]},
              permanent, 
              5000,
              worker,
              [stolas_server]
             }||X<-lists:seq(1, ThreadNum)],
            Res=supervisor:start_child(
                  stolas_sup,
                  {Task,
                   {stolas_simple_sup, start_link, [Task,
                                                    [MasterSpec|ChildSpecs]]},
                   permanent, infinity, supervisor, [stolas_simple_sup]}),
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

handle_cast(map, State) when is_record(State, worker_state)->
    #worker_state{
       workspace=Workspace,
       mod=Mod,
       master=Master,
       name=Name
      }=State,
    try
        case apply(Mod, map, [Workspace]) of
            ok->
                gen_server:cast(Master, {reduce, {ok, Name}});
            {error, Reason}->
                gen_server:cast(Master, {reduce, {error, Name, Reason}})
        end
    catch
        T:R->gen_server:cast(Master, {reduce, {error, Name, {T, R}}})
    end,
    {noreply, State};

handle_cast({init, InitArgs}, State) when is_record(State, master_state)->
    #master_state{
       task=Task,
       thread_num=ThreadNum,
       workspace=Workspace,
       mod=Mod
      }=State,
    try
        case apply(Mod, init, [Workspace, InitArgs]) of
            ok->
                lists:foreach(fun(X)->
                                      file:make_dir(
                                        ?sub_workspace(Workspace,
                                                       integer_to_list(X)))
                              end, lists:seq(1, ThreadNum)),
                ?broadcast_map_msg(Task, ThreadNum);
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
handle_cast({reduce, {ok, Name}}, State) when is_record(State, master_state)->
    #master_state{
       finish_tasks=FinishTasks,
       mod=Mod,
       workspace=Workspace,
       thread_num=ThreadNum,
       task=Task
      }=State,
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
handle_cast({reduce, {error, Name, Reason}}, State)
  when is_record(State, master_state)->
    Task=State#master_state.task,
    error_logger:info_msg("Task:~p, Worker:~p map failed", [Task, Name]),
    gen_server:cast(stolas_manager, {close_task, Task,
                                     #failure_msg{
                                        task=Task,
                                        role=worker,
                                        reason=Reason,
                                        msg="Task map failed"
                                       }}),
    {noreply, State};

handle_cast({close_task, Task, Res}, State)
  when is_record(State, manager_state)->
    TaskSets=State#manager_state.task_sets,
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

terminate(_Reason, State) when is_record(State, manager_state)->
    timer:cancel(State#manager_state.ping_tref),
    ok;
terminate(_Reason, _State)->
    ok.
