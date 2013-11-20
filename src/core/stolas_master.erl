-module(stolas_master).
-behaviour(gen_server).

-include("stolas.hrl").

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-define(broadcast_workers_msg(Task, ThreadNum, Msg),
        lists:foreach(fun(X)->
                              gen_server:cast(?task_id(Task, X), Msg)
                      end, lists:seq(1, ThreadNum))).
-define(main_worker(Task), ?task_id(Task, 0)).
-define(alloc_task_dict_name(Task),
        list_to_atom(lists:concat(["stolas_master:alloc_task:", Task]))).
-define(close_task(Task, Reason),
        gen_server:cast(stolas_manager, {close_task, Task, Reason})).

-record(master_state, {
          task::atom(),
          leader::atom(),
          mod::atom(),
          resources::list(),
          thread_num::integer(),
          alloc_task_dict::term(),
          alloc_end::true|false,
          work_results_log::term(),
          finish_thread::integer(),
          status::init|map|reduce|idle|interrupt|completed
         }).

start_link(RegName, Opt)->
    gen_server:start_link({local, RegName}, ?MODULE, [Opt], []).

init([Opt])->
    process_flag(trap_exit, true),
    Task=proplists:get_value(task, Opt),
    WorkResultFile=proplists:get_value(work_result_file, Opt),
    file:delete(WorkResultFile),
    case disk_log:open([{name, lists:concat([Task, ":worklog"])},
                        {file, WorkResultFile}]) of
        {error, Err}->
            {stop, {"create work log failed", Err}};
        {ok, Log}->
            ThreadNum=proplists:get_value(thread_num, Opt),
            Leader=proplists:get_value(leader, Opt),
            Mod=proplists:get_value(mod, Opt),
            Resources=proplists:get_value(resources, Opt),
            {ok, #master_state{
                    task=Task,
                    leader=Leader,
                    mod=Mod,
                    resources=Resources,
                    thread_num=ThreadNum,
                    alloc_task_dict=?dict_new(?alloc_task_dict_name(Task)),
                    work_results_log=Log,
                    alloc_end=false,
                    finish_thread=0,
                    status=idle
                   }}
    end.

handle_call(get_main_worker, _From, State=#master_state{
                                             leader=Leader,
                                             task=Task
                                            })->
    {reply, {?main_worker(Task), Leader}, State};
handle_call(get_mod, _From, State)->
    {reply, State#master_state.mod, State};
handle_call(get_status, _From, State)->
    {reply, State#master_state.status, State};
handle_call(_Msg, _From, State)->
    {reply, {error, "error message"}, State}.

handle_cast(check_leader, State=#master_state{
                                   task=Task,
                                   leader=Leader,
                                   thread_num=ThreadNum,
                                   resources=Resources,
                                   status=Status
                                  }) when Leader=/=node()->
    try
        NewStatus=
        case {gen_server:call({?task_id(Task, master), Leader},
                              get_status),
              Status} of
            {init, idle}->
                idle;
            {map, idle}->
                stolas_file:get_resources(Resources, Leader),
                ?broadcast_workers_msg(Task, ThreadNum, map),
                map;
            {S, _}->S
        end,
        timer:apply_after(1000, gen_server, cast, [self(), check_leader]),
        {noreply, State#master_state{status=NewStatus}}
    catch
        exit:{noproc, _}->
            ?close_task(Task, normal),
            {noreply, State};
        _:_->
            ?close_task(Task, force),
            {noreply, State}
    end;
handle_cast({init, InitArgs, Acc}, State=#master_state{
                                            mod=Mod,
                                            task=Task,
                                            leader=Leader,
                                            status=idle
                                           })->
    if
        Leader=:=node()->gen_server:cast(?main_worker(Task),
                                         {init, Mod, InitArgs, Acc});
        true->gen_server:cast(self(), wait_leader)
    end,
    {noreply, State#master_state{status=init}};
handle_cast(Msg={worker_msg, _Type, _Process, _Detail},
            State=#master_state{
                     task=Task,
                     leader=Leader
                    }) when Leader=/=node()->
    gen_server:cast({?task_id(Task, master), Leader}, Msg),
    {noreply, State};
handle_cast({worker_msg, error, Process, Detail}, State)->
    ?close_task(State#master_state.task,
                {error, stolas_msg:process_worker_err_msg(Process, Detail)}),
    {noreply, State};
handle_cast({worker_msg, ok, init, _WorkerName},
            State=#master_state{
                     leader=Leader,
                     task=Task,
                     thread_num=ThreadNum,
                     status=init
                    }) when Leader=:=node()->
    error_logger:info_msg("Task:~p init ok", [Task]),
    ?broadcast_workers_msg(Task, ThreadNum, map),
    {noreply, State#master_state{status=map}};
handle_cast({worker_msg, ok, alloc, {WorkerName, Node, Args}},
            State=#master_state{
                     leader=Leader,
                     alloc_task_dict=AllocTaskDict,
                     alloc_end=false,
                     status=map
                    }) when Leader=:=node()->
    {noreply, State#master_state{
                      alloc_task_dict=?dict_add(AllocTaskDict,
                                                {WorkerName, Node}, Args)
                     }};
handle_cast({worker_msg, 'end', alloc, null},
            State=#master_state{
                     task=Task,
                     leader=Leader,
                     alloc_end=false,
                     status=map
                    }) when Leader=:=node()->
    error_logger:info_msg("Task:~p alloc end", [Task]),
    {noreply, State#master_state{alloc_end=true}};
handle_cast({worker_msg, ok, map, {Name, TaskArgs, Result}},
            State=#master_state{
                     task=Task,
                     leader=Leader,
                     alloc_task_dict=AllocTaskDict,
                     work_results_log=WorkResultLog,
                     status=map
                    }) when Leader=:=node()->
    error_logger:info_msg("Task:~p map ok. worker:~p", [Task, Name]),
    NewAllocTaskDict=?dict_del(AllocTaskDict, Name),
    disk_log:blog(WorkResultLog, term_to_binary({Name, TaskArgs, Result})),
    {noreply, State#master_state{alloc_task_dict=NewAllocTaskDict}};
handle_cast(Msg={worker_msg, ok, map, {_Name, _TaskArgs, _Result}},
            State=#master_state{
                     task=Task,
                     leader=Leader,
                     status=map
                    }) when Leader=/=node()->
    gen_server:cast({?task_id(Task, master), Leader}, Msg),
    {noreply, State};
handle_cast({worker_msg, 'end', map, Name},
            State=#master_state{
                     task=Task,
                     mod=Mod,
                     alloc_task_dict=AllocTaskDict,
                     status=map
                    })->
    error_logger:info_msg("Task:~p map end, worker:~p", [Task, Name]),
    NewStatus=case ?dict_size(AllocTaskDict) of
                  0->
                      gen_server:cast(?main_worker(Task), {reduce, Mod}),
                      reduce;
                  _->map
              end,
    {noreply, State#master_state{status=NewStatus}};
handle_cast({worker_msg, ok, reduce, {_WorkerName, _Result}},
            State=#master_state{
                     task=Task,
                     leader=Leader,
                     status=reduce
                    }) when Leader=:=node()->
    error_logger:info_msg("Task:~p reduce ok", [Task]),
    ?close_task(Task, normal),
    {noreply, State#master_state{status=completed}};
handle_cast(_Msg, State)->
    {noreply, State}.

handle_info(_Msg, State)->
    {noreply, State}.

code_change(_Vsn, State, _Extra)->
    {ok, State}.

terminate(_Reason, #master_state{
                      alloc_task_dict=_AllocTaskDict,
                      work_results_log=WorkResultLog
                     })->
    disk_log:close(WorkResultLog),
    ?dict_drop(_AllocTaskDict),
    ok.
