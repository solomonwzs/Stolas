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

-record(master_state, {
          task::atom(),
          leader::atom(),
          mod::atom(),
          workspace::string(),
          sub_num::integer()|null,
          thread_num::integer(),
          work_results::term(),
          finish_tasks::integer()
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
            Workspace=proplists:get_value(workspace, Opt),
            Mod=proplists:get_value(mod, Opt),
            ThreadNum=proplists:get_value(thread_num, Opt),
            {ok, #master_state{
                    mod=Mod,
                    workspace=Workspace,
                    thread_num=ThreadNum,
                    work_results=Log,
                    task=Task,
                    finish_tasks=0
                   }}
    end.

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
                                 #task_failure_msg{
                                    task=Task,
                                    role=master,
                                    reason=Reason,
                                    msg="Task initialization failed"
                                   }})
        end
    catch
        T:R->gen_server:cast(stolas_manager,
                             {close_task, Task,
                              #task_failure_msg{
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
                                         #task_failure_msg{
                                            task=Task,
                                            role=master,
                                            reason=Reason,
                                            msg="Task reduce failed"
                                           }})
                end
            catch
                T:R->gen_server:cast(stolas_manager,
                                     {close_task, Task,
                                      #task_failure_msg{
                                         task=Task,
                                         role=master,
                                         reason={T, R},
                                         msg="unexpected error"
                                        }})
            end;
        true->ok
    end,
    {noreply, State#master_state{finish_tasks=FinishTasks+1}};
handle_cast({map_ok, Name, TaskArgs, Result}, State=#master_state{
                                                       task=Task,
                                                       work_results=WorkResult
                                                      })->
    error_logger:info_msg("Task:~p, Worker:~p map ok", [Task, Name]),
    disk_log:blog(WorkResult, term_to_binary({Name, TaskArgs, Result})),
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
                      work_results=WorkResult
                     })->
    disk_log:close(WorkResult),
    ok.
