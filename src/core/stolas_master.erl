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

-record(master_state, {
          task::atom(),
          leader::atom(),
          mod::atom(),
          workspace::string(),
          sub_num::integer()|null,
          thread_num::integer(),
          work_results_log::term(),
          finish_tasks::integer(),
          status::init|map|reduce|idle|interrupt|completed
         }).

start_link(RegName, Opt)->
    gen_server:start_link({local, RegName}, ?MODULE, [Opt], []).

init([Opt])->
    process_flag(trap_exit, true),
    Task=proplists:get_value(task, Opt),
    WorkResultFile=proplists:get_value(work_result, Opt),
    case disk_log:open([{name, lists:concat([Task, ":worklog"])},
                        {file, WorkResultFile}]) of
        {error, Err}->
            {stop, {"create work log failed", Err}};
        {ok, Log}->
            ThreadNum=proplists:get_value(thread_num, Opt),
            Leader=proplists:get_value(leader, Opt),
            {ok, #master_state{
                    thread_num=ThreadNum,
                    work_results_log=Log,
                    task=Task,
                    leader=Leader,
                    finish_tasks=0,
                    status=idle
                   }}
    end.

handle_call(get_status, _From, State)->
    {reply, State#master_state.status, State};
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
                             #task_failure_msg{
                                task=Task,
                                role=master,
                                reason={T, R},
                                msg="unexpected error"
                               }}),
            {reply, error, State}
    end;
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
                      map
              end,
    {noreply, State#master_state{status=NewStatus}};
handle_cast(Msg={init, _InitArgs}, State=#master_state{
                                            task=Task,
                                            leader=Leader,
                                            status=idle
                                           })->
    if
        Leader=:=node()->gen_server:cast(?main_worker(Task), Msg);
        true->gen_server:cast(self(), wait_leader)
    end,
    {noreply, State#master_state{status=init}};
handle_cast({init_complete, Res}, State=#master_state{
                                           leader=Leader,
                                           task=Task,
                                           status=init
                                          }) when Leader=:=node()->
    NewStatus=case Res of
                  {ok, _WorkerName}->map;
                  {failed, _WorkerName, Reason}->
                      gen_server:cast(stolas_manager, {close_task, Task,
                                                       #task_failure_msg{
                                                          task=Task,
                                                          reason=Reason
                                                         }}),
                      interrupt
              end,
    {noreply, State#master_state{status=NewStatus}};
%handle_cast({reduce_complete, Res}, State=#master_state{
%                                             status=reduce
%                                            })->
%    NewStatus=case Res of
%                  {ok, _WorkerName, _Result}->
%                      gen_server:cast(stolas_manager,)
handle_cast({map_ok, Name, TaskArgs, Result},
            State=#master_state{
                     task=Task,
                     work_results_log=WorkResultLog
                    })->
    error_logger:info_msg("Task:~p, Worker:~p map ok", [Task, Name]),
    disk_log:blog(WorkResultLog, term_to_binary({Name, TaskArgs, Result})),
    {noreply, State};
handle_cast({map_error, Name, Reason}, State=#master_state{
                                                task=Task
                                               })->
    error_logger:info_msg("Task:~p, Worker:~p map failed", [Task, Name]),
    gen_server:cast(stolas_manager, {close_task, Task,
                                     #task_failure_msg{
                                        task=Task,
                                        role=worker,
                                        reason=Reason,
                                        msg="Task map failed"
                                       }}),
    {noreply, State};
handle_cast(_Msg, State)->
    {noreply, State}.

handle_info(_Msg, State)->
    {noreply, State}.

code_change(_Vsn, State, _Extra)->
    {ok, State}.

terminate(_Reason, #master_state{
                      work_results_log=WorkResultLog
                     })->
    disk_log:close(WorkResultLog),
    ok.
