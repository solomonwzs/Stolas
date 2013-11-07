-module(stolas_manager).
-behaviour(gen_server).

-include("stolas.hrl").

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-record(manager_state, {
          task_dict::tuple(),
          ping_tref::term(),
          master_node::atom(),
          config::list(tuple())
         }).

-define(PING_INTERVAL, 1500).

-define(sub_workspace(Workspace, X), filename:join(Workspace, X)).
-define(ping_tref(Nodes),
        timer:apply_interval(
          ?PING_INTERVAL,
          lists, foreach, [fun(Node)->
                                   net_adm:ping(Node)
                           end, Nodes])).
-define(valid_nodes(Conf),
        sets:to_list(sets:intersection(
                       sets:from_list(proplists:get_value(nodes, Conf)),
                       sets:from_list(nodes())
                      ))).
-define(master_spec(MasterId, ThreadNum, Mod, Workspace, Task, Leader),
        {
         MasterId,
         {stolas_master, start_link,
          [MasterId, [{thread_num, ThreadNum},
                      {leader, Leader},
                      {work_result_file,
                       filename:join(
                         Workspace, "log/work_result")},
                      {mod, Mod}, 
                      {task, Task}]]},
         permanent, 5000, worker, [stolas_master]
        }).
-define(worker_specs(MasterId, ThreadNum, Mod, Workspace, Task),
        lists:map(fun(X)->
                          {?task_id(Task, X),
                           {stolas_worker, start_link,
                            [?task_id(Task, X), [{master, MasterId},
                                                 {workspace, 
                                                  ?sub_workspace(
                                                     Workspace,
                                                     integer_to_list(X))}]]},
                           permanent, 5000, worker, [stolas_worker]
                          }
                  end, lists:seq(0, ThreadNum))).
-define(start_task(Node, Task, ChildSpecs),
        supervisor:start_child(
          {stolas_sup, Node},
          {Task,
           {stolas_simple_sup, start_link, [Task, ChildSpecs]},
           permanent, infinity, supervisor, [stolas_simple_sup]})).
-define(split_thread_alloc(LeaderNode, ThreadAlloc),
        {proplists:lookup(LeaderNode, ThreadAlloc),
         proplists:delete(LeaderNode, ThreadAlloc)}).

start_link(RegName, Conf)->
    gen_server:start_link({local, RegName}, ?MODULE, [Conf], []).

init([Conf])->
    process_flag(trap_exit, true),
    {PingTref, MasterNode}=process_conf(Conf),
    {ok, #manager_state{
            task_dict=?dict_new('stolas:task_dict'),
            ping_tref=PingTref,
            master_node=MasterNode,
            config=Conf
           }}.

handle_call(get_config, _From, State=#manager_state{
                                        master_node=MasterNode,
                                        config=Conf
                                       })->
    if
        MasterNode=:=node()->
            {reply, {ok, Conf}, State};
        true->
            Ret=gen_server:call({stolas_manager, MasterNode}, get_config),
            {reply, Ret, State}
    end;
handle_call(reload_config, _From, State=#manager_state{
                                           master_node=MasterNode,
                                           task_dict=TaskDict
                                          }) when MasterNode=:=node()->
    case ?dict_size(TaskDict) of
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
handle_call(get_state, _From, State)->
    {reply, {ok, State}, State};
handle_call({new_task, Opt}, _From,
            State=#manager_state{
                     task_dict=TaskDict,
                     config=Conf,
                     master_node=MasterNode
                    }) when MasterNode=:=node()->
    Task=proplists:get_value(task, Opt),
    case ?dict_find(TaskDict, Task) of
        {ok, _}->
            {reply, {error, "already existed"}, State};
        error->
            Task=proplists:get_value(task, Opt),
            Mod=proplists:get_value(mod, Opt),
            Workspace=proplists:get_value(workspace, Opt),
            LeaderNode=proplists:get_value(leader_node, Opt),

            ValidNodes=?valid_nodes(Conf),
            ValidAlloc=lists:filter(
                         fun({N, _})->
                                 lists:member(N, ValidNodes)
                         end, proplists:get_value(thread_alloc, Opt)),
            case ?split_thread_alloc(LeaderNode, ValidAlloc) of
                {none, _}->{reply, {error, "leader node not existed"}, State};
                {{LeaderNode, LeaderThreadNum}, SubAlloc}->
                    case new_task(LeaderNode, LeaderThreadNum, Mod, Workspace,
                                  Task, LeaderNode) of
                        Err={error, _}->
                            {reply, Err, State};
                        {ok, _, MasterId}->
                            NewTaskDict=?dict_add(TaskDict, Task, LeaderNode),
                            InitArgs=proplists:get_value(init_args, Opt),
                            gen_server:cast({MasterId, node()},
                                            {init, Mod, InitArgs}),
                            lists:foreach(
                              fun({N, T})->
                                      new_task(N, T, Mod, Workspace, Task,
                                               LeaderNode)
                              end, SubAlloc),
                            {reply, ok, State#manager_state{
                                          task_dict=NewTaskDict}}
                    end
            end
    end;
handle_call(_Msg, _From, State)->
    {reply, {error, "error message"}, State}.

handle_cast(sync_state, State=#manager_state{
                                 master_node=MasterNode
                                }) when MasterNode=/=node()->
    NewState=case gen_server:call({stolas_manager, MasterNode}, get_state) of
                 {ok, S}->S;
                 _->State
             end,
    {noreply, NewState};
handle_cast({close_task, Task, Res}, State=#manager_state{
                                              task_dict=TaskDict
                                             })->
    case ?dict_find(TaskDict, Task) of
        {ok, _LeaderNode}->
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
                       worker=Worker,
                       reason=Reason,
                       msg=Msg
                      }=Res,
                    error_logger:error_msg(
                      "Task:~p failed, worker:~p, reason:~p, msg:~p",
                      [Task, Worker, Reason, Msg])
            end,
            {noreply, State#manager_state{
                        task_dict=?dict_del(TaskDict, Task)}};
        error->
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
    MasterNode=proplists:get_value(master_node, Conf),
                 
    {PingTref, MasterNode}.

cancel_conf(#manager_state{
               task_dict=TaskDict,
               ping_tref=PingTref
              })->
    timer:cancel_conf(PingTref),
    ?dict_drop(TaskDict),
    error_logger:delete_report_handler(stolas_log_handler),
    ok.

new_task(Node, ThreadNum, Mod, Workspace, Task, LeaderNode)->
    MasterId=?task_id(Task, master),
    MasterSpec=?master_spec(MasterId, ThreadNum, Mod,
                            Workspace, Task, LeaderNode),
    WorkerSpecs=?worker_specs(MasterId, ThreadNum, Mod, Workspace,
                              Task),
    case ?start_task(Node, Task, [MasterSpec|WorkerSpecs]) of
        {ok, Pid}->{ok, Pid, MasterId};
        Err={error, _}->Err
    end.
