-module(stolas_manager).
-behaviour(gen_server).

-include("stolas.hrl").

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-record(manager_state, {
          status::ok|waiting
         }).

-define(PING_INTERVAL, 1500).

-define(get_archive, gen_server:call(stolas_archive, get_archive)).
-define(get_and_lock_archive, gen_server:call(stolas_archive,
                                              {get_archive, self(), write})).
-define(set_and_unlock_archive(Archive),
        gen_server:call(stolas_archive,
                        {set_archive, Archive, self(), write})).

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
                       sets:from_list([node()|nodes()])
                      ))).
-define(master_spec(MasterId, ThreadNum, Mod, Workspace, Task, Leader,
                    Resources),
        {
         MasterId,
         {stolas_master, start_link,
          [MasterId, [{thread_num, ThreadNum},
                      {leader, Leader},
                      {work_result_file,
                       filename:join(
                         Workspace, "log/work_result")},
                      {mod, Mod},
                      {resources, Resources},
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
-define(start_task(Task, ChildSpecs),
        supervisor:start_child(
          stolas_sup,
          {Task,
           {stolas_simple_sup, start_link, [Task, ChildSpecs]},
           permanent, infinity, supervisor, [stolas_simple_sup]})).
-define(split_thread_alloc(LeaderNode, ThreadAlloc),
        {proplists:lookup(LeaderNode, ThreadAlloc),
         proplists:delete(LeaderNode, ThreadAlloc)}).
-define(new_sub_task(Node, MasterId, ThreadNum, Mod, Workspace, Task,
                     LeaderNode, Resources),
        case gen_server:call({stolas_manager, Node},
                             {new_sub_task, ThreadNum,
                              Mod, Workspace, Task, LeaderNode, Resources}) of
            ok->gen_server:cast({MasterId, Node}, check_leader);
            Err={error, _}->Err
        end).


start_link(RegName)->
    gen_server:start_link({local, RegName}, ?MODULE, [], []).


init([])->
    process_flag(trap_exit, true),
    case ?get_archive of
        {ok, _}->
            {ok, #manager_state{status=ok}};
        {error, _}->
            gen_server:cast(self(), wait_archive),
            {ok, #manager_state{status=waiting}}
    end.


handle_call(get_state, _From, State)->
    {reply, {ok, State}, State};
handle_call({new_sub_task, ThreadNum, Mod, Workspace, Task, LeaderNode,
             Resources}, _From, State=#manager_state{
                                         status=ok
                                        })->
    {ok, #archive{
            task_dict=TaskDict
           }}=?get_archive,
    Reply=case ?dict_find(TaskDict, Task) of
              {ok, _}->
                  case new_task(ThreadNum, Mod, Workspace, Task, LeaderNode,
                                Resources) of
                      Err={error, _}->Err;
                      {ok, _, _}->ok
                  end;
              error->{error, task_not_existed}
          end,
    {reply, Reply, State};
handle_call({new_task, Opt, LeaderNode}, _From, State=#manager_state{
                                                         status=ok
                                                        })->
    {ok, Archive=#archive{
                    task_dict=TaskDict,
                    config=Conf
                   }}=?get_and_lock_archive,
    Task=proplists:get_value(task, Opt),
    Reply=
    case ?dict_find(TaskDict, Task) of
        {ok, _}->
            ?set_and_unlock_archive(Archive),
            {error, task_already_existed};
        error->
            Mod=proplists:get_value(mod, Opt),
            Workspace=proplists:get_value(workspace, Opt),
            Resources=proplists:get_value(resources, Opt),

            file:make_dir(filename:join(Workspace, "log")),

            ValidNodes=?valid_nodes(Conf),
            ValidAlloc=lists:filter(
                         fun({N, _})->
                                 lists:member(N, ValidNodes)
                         end, proplists:get_value(thread_alloc, Opt)),
            case ?split_thread_alloc(LeaderNode, ValidAlloc) of
                {none, _}->{error, leader_not_existed};
                {{LeaderNode, LeaderThreadNum}, SubAlloc}->
                    case new_task(LeaderThreadNum, Mod, Workspace,
                                  Task, LeaderNode, Resources) of
                        Err={error, _}->
                            {reply, Err, State};
                        {ok, _, MasterId}->
                            InitArgs=proplists:get_value(init_args, Opt),
                            gen_server:cast({MasterId, node()},
                                            {init, InitArgs}),
                            ?set_and_unlock_archive(
                               Archive#archive{
                                 task_dict=?dict_add(TaskDict, Task,
                                                     LeaderNode)
                                }),
                            lists:foreach(
                              fun({N, T})->
                                      R=?new_sub_task(N, MasterId, T, Mod,
                                                      Workspace, Task,
                                                      LeaderNode, Resources),
                                      ?debug_log(R)
                              end, SubAlloc)
                    end
            end
    end,
    {reply, Reply, State};
handle_call(_Msg, _From, State)->
    {reply, {error, error_message}, State}.


handle_cast(wait_archive, State=#manager_state{
                                   status=waiting
                                  })->
    case ?get_archive of
        {ok, _}->
            {noreply, State#manager_state{status=ok}};
        {error, _}->
            ?cast_self_after(1000, wait_archive),
            {noreply, State}
    end;
handle_cast(Msg={close_task, Task, Reason}, State=#manager_state{
                                                     status=ok
                                                    })->
    case ?get_and_lock_archive of
        {ok, Archive=#archive{
                        task_dict=TaskDict
                       }}->
            NewTaskDict=
            case ?dict_find(TaskDict, Task) of
                {ok, _LeaderNode}->
                    case Reason of
                        normal->
                            error_logger:info_report(
                              [{task, Task},
                               {msg, "completed"}
                              ]);
                        force->
                            error_logger:info_report(
                              [{task, Task},
                               {msg, "closed forcibly"}
                              ]);
                        {error, Err}->
                            error_logger:info_report(
                              [{task, Task},
                               {error, Err}
                              ])
                    end,
                    ?dict_del(TaskDict, Task);
                error->TaskDict
            end,
            supervisor:terminate_child(stolas_sup, Task),
            supervisor:delete_child(stolas_sup, Task),
            ?set_and_unlock_archive(Archive#archive{task_dict=NewTaskDict});
        _->?cast_self_after(1000, Msg)
    end,
    {noreply, State};
handle_cast(_Msg, State)->
    {noreply, State}.


handle_info(_Msg, State)->
    {noreply, State}.


code_change(_Vsn, State, _Extra)->
    {ok, State}.


terminate(_Reason, _State)->
    ok.


new_task(ThreadNum, Mod, Workspace, Task, LeaderNode, Resources)->
    MasterId=?task_id(Task, master),
    MasterSpec=?master_spec(MasterId, ThreadNum, Mod, Workspace, Task,
                            LeaderNode, Resources),
    WorkerSpecs=?worker_specs(MasterId, ThreadNum, Mod, Workspace, Task),
    case ?start_task(Task, [MasterSpec|WorkerSpecs]) of
        {ok, Pid}->{ok, Pid, MasterId};
        Err={error, _}->Err
    end.
