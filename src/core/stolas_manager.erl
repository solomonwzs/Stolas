-module(stolas_manager).
-behaviour(gen_server).

-include("stolas.hrl").

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-define(PING_INTERVAL, 1500).

-define(sub_workspace(Workspace, X), filename:join(Workspace, X)).
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
          {stolas_master, start_link,
           [MasterId, [{thread_num, ThreadNum},
                       {work_result_file, filename:join(
                                            Workspace, "log/work_result")},
                       {mod, Mod}, {task, Task},
                       {workspace, ?sub_workspace(Workspace, "master")},
                       {valid_nodes, ?get_valid_nodes(ConfNodes)}]]},
          permanent, 5000, worker, [stolas_master]
         }).
-define(worker_specs(MasterId, ThreadNum, Mod, Workspace, Task),
        lists:map(fun(X)->
                          {?task_id(Task, X),
                           {stolas_worker, start_link,
                            [?task_id(Task, X), [{mod, Mod},
                                            {master, MasterId},
                                            {workspace, 
                                             ?sub_workspace(
                                                Workspace,
                                                integer_to_list(X))}]]},
                           permanent, 5000, worker, [stolas_worker]
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

start_link(RegName, Conf)->
    gen_server:start_link({local, RegName}, ?MODULE, [Conf], []).

init([Conf])->
    process_flag(trap_exit, true),
    {PingTref, IsMaster}=process_conf(Conf),
    {ok, #manager_state{
            task_sets=sets:new(),
            ping_tref=PingTref,
            is_master=IsMaster,
            config=Conf
           }}.

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
handle_call(Msg={new_task, Opt}, _From,
            State=#manager_state{
                     task_sets=TaskSets,
                     is_master=_IsMaster,
                     config=Conf
                    })->
    Task=proplists:get_value(task, Opt),
    case sets:is_element(Task, TaskSets) of
        true->
            {reply, {error, "already existed"}, State};
        false->
            Mod=proplists:get_value(mod, Opt),
            Workspace=proplists:get_value(workspace, Opt),
            MasterId=?task_id(Task, master),

            MasterNode=proplists:get_value(Conf, master),
            LocalNode=node(),
            ConfNodes=proplists:get_value(Conf, nodes, []),
            ValidNodes=?get_valid_nodes(ConfNodes),
            if
                MasterNode=:=LocalNode->
                    TaskAlloc1=proplists:get_value(Opt, task_alloc),
                    TaskAlloc2=
                    lists:foldl(
                      fun(N={X, _}, Acc)->
                              case lists:member(X, ValidNodes)
                                   andalso MasterNode=/=X of
                                  true->
                                      case gen_server:call(
                                             {stolas_manager, X},
                                             Msg) of
                                          {ok, _}->[N|Acc];
                                          _->Acc
                                      end;
                                  false->Acc
                              end
                      end, [], TaskAlloc1),
                    TaskAlloc3=case proplists:lookup(LocalNode,
                                                        TaskAlloc1) of
                                   none->[{LocalNode, 0}|TaskAlloc2];
                                   T->[T|TaskAlloc2]
                               end,
                    ThreadNum=lists:sum([X||{_, X}<-TaskAlloc3]),
                    MasterSpec=?master_spec(MasterId, ThreadNum, Mod, Workspace,
                                            Task, ConfNodes),
                    WorkerSpecs=?worker_specs(MasterId, ThreadNum, Mod,
                                              Workspace, Task),
                    Res=?start_task(Task, [MasterSpec|WorkerSpecs]),
                    NewTaskSets=sets:add_element(Task, TaskSets),
                    InitArgs=proplists:get_value(init_args, Opt),
                    gen_server:cast(MasterId, {init, InitArgs}),
                    {reply, Res, State#manager_state{
                                   task_sets=NewTaskSets}};
                true->
                    ok
            end
    end;
handle_call(_Msg, _From, State)->
    {reply, {error, "error message"}, State}.

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
                is_record(Res, task_failure_msg)->
                    #task_failure_msg{
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

terminate(_Reason, #manager_state{
                             ping_tref=PingTref
                            })->
    timer:cancel(PingTref),
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
