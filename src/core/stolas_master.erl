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

-record(master_state, {
          task::atom(),
          leader::atom(),
          mod::atom(),
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
    case disk_log:open([{name, lists:concat([Task, ":worklog"])},
                        {file, WorkResultFile}]) of
        {error, Err}->
            {stop, {"create work log failed", Err}};
        {ok, Log}->
            ThreadNum=proplists:get_value(thread_num, Opt),
            Leader=proplists:get_value(leader, Opt),
            Mod=proplists:get_value(mod, Opt),
            {ok, #master_state{
                    task=Task,
                    leader=Leader,
                    mod=Mod,
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

handle_cast(wait_leader, State=#master_state{
                                  task=Task,
                                  leader=Leader,
                                  thread_num=ThreadNum,
                                  status=init
                                 }) when Leader=/=node()->
    NewStatus=case gen_server:call({stolas_master, Leader}, get_status) of
                  init->
                      timer:apply_after(1000, gen_server, cast,
                                        [self(), wait_leader]),
                      init;
                  map->
                      ?broadcast_workers_msg(Task, ThreadNum, map),
                      map;
                  S->S
              end,
    {noreply, State#master_state{status=NewStatus}};
handle_cast({init, InitArgs}, State=#master_state{
                                            mod=Mod,
                                            task=Task,
                                            leader=Leader,
                                            status=idle
                                           })->
    if
        Leader=:=node()->gen_server:cast(?main_worker(Task),
                                         {init, Mod, InitArgs});
        true->gen_server:cast(self(), wait_leader)
    end,
    {noreply, State#master_state{status=init}};
handle_cast({init_ok, _WorkerName}, State=#master_state{
                                           leader=Leader,
                                           task=Task,
                                           thread_num=ThreadNum,
                                           status=init
                                          }) when Leader=:=node()->
    ?broadcast_workers_msg(Task, ThreadNum, map),
    {noreply, State#master_state{status=map}};
handle_cast({init_error, WorkerName, Reason}, State=#master_state{
                                                        task=Task,
                                                        mod=Mod,
                                                        leader=Leader,
                                                        status=init
                                                       }) when Leader=:=node()->
    gen_server:cast(stolas_manager, {close_task, Task, 
                                     #task_failure_msg{
                                        worker={WorkerName, Leader},
                                        mfc={Mod, init, []},
                                        reason=Reason,
                                        msg="init error"
                                       }}),
    {noreply, State#master_state{status=interrupt}};
handle_cast({new_alloc, WorkerName, Node, Args},
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
handle_cast(alloc_end, State=#master_state{
                                leader=Leader,
                                alloc_end=false,
                                status=map
                               }) when Leader=:=node()->
    {noreply, State#master_state{alloc_end=true}};
handle_cast({map_ok, Name, TaskArgs, Result},
            State=#master_state{
                     leader=Leader,
                     alloc_task_dict=AllocTaskDict,
                     work_results_log=WorkResultLog,
                     status=map
                    }) when Leader=:=node()->
    NewAllocTaskDict=?dict_del(AllocTaskDict, Name),
    disk_log:blog(WorkResultLog, term_to_binary({Name, TaskArgs, Result})),
    {noreply, State#master_state{alloc_task_dict=NewAllocTaskDict}};
handle_cast(Msg={map_ok, _Name, _TaskArgs, _Result},
            State=#master_state{
                     task=Task,
                     leader=Leader,
                     status=map
                    }) when Leader=/=node()->
    gen_server:cast({?task_id(Task, master), Leader}, Msg),
    {noreply, State};
handle_cast({map_error, Name, TaskArgs, Reason},
            State=#master_state{
                     leader=Leader,
                     mod=Mod,
                     task=Task,
                     status=map
                    }) when Leader=:=node()->
    gen_server:cast(stolas_manager, {close_task, Task,
                                     #task_failure_msg{
                                        worker=Name,
                                        reason=Reason,
                                        mfc={Mod, map, TaskArgs},
                                        msg="Task map failed"
                                       }}),
    {noreply, State};
handle_cast(Msg={map_error, _Name, _TaskArgs, _Reason},
            State=#master_state{
                     task=Task,
                     leader=Leader,
                     status=map
                    }) when Leader=/=node()->
    gen_server:cast({?task_id(Task, master)}, Msg),
    {noreply, State};
handle_cast({work_end, _WorkerName}, State=#master_state{
                                                 leader=Leader,
                                                 thread_num=ThreadNum,
                                                 finish_thread=Finish,
                                                 status=map
                                                })->
    NewFinish=Finish+1,
    if
        NewFinish=:=ThreadNum->
            gen_server:cast({stolas_master, Leader}, map_end);
        true->ok
    end,
    {noreply, State#master_state{finish_thread=NewFinish}};
handle_cast(map_end, State=#master_state{
                              task=Task,
                              leader=Leader,
                              mod=Mod,
                              alloc_task_dict=AllocTaskDict,
                              status=map
                             }) when Leader=:=node()->
    NewStatus=case ?dict_size(AllocTaskDict) of
                  0->
                      gen_server:cast(?main_worker(Task), {reduce, Mod}),
                      reduce;
                  _->map
              end,
    {noreply, State#master_state{status=NewStatus}};
handle_cast({reduce_ok, _WorkerName, _Result},
            State=#master_state{
                     task=Task,
                     leader=Leader,
                     status=reduce
                    }) when Leader=:=node()->
    gen_server:cast(stolas_manager, {close_task, Task, normal}),
    {noreply, State#master_state{status=completed}};
handle_cast({reduce_error, WorkerName, Reason},
            State=#master_state{
                     task=Task,
                     mod=Mod,
                     leader=Leader,
                     status=reduce
                    }) when Leader=:=node()->
    gen_server:cast(stolas_manager, {close_task, Task,
                                     #task_failure_msg{
                                        worker={WorkerName, node},
                                        mfc={Mod, reduce, []},
                                        reason=Reason,
                                        msg="reduce failed"
                                       }}),
    {noreply, State#master_state{status=interrupt}};
handle_cast(_Msg, State)->
    {noreply, State}.

handle_info(_Msg, State)->
    {noreply, State}.

code_change(_Vsn, State, _Extra)->
    {ok, State}.

terminate(_Reason, #master_state{
                      alloc_task_dict=AllocTaskDict,
                      work_results_log=WorkResultLog
                     })->
    disk_log:close(WorkResultLog),
    ?dict_drop(AllocTaskDict),
    ok.
