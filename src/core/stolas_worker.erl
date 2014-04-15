-module(stolas_worker).
-behaviour(gen_server).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-record(worker_state, {
          name::atom(),
          workspace::string(),
          master::atom(),
          acc::undefined|term()
         }).

-define(TRACE, {trace, erlang:get_stacktrace()}).

-define(task_mod(Master), gen_server:call(Master, get_mod)).
-define(task_main_worker(Master), gen_server:call(Master, get_main_worker)).
-define(send_msg_to_master(Master, Type, Process, Detail),
        gen_server:cast(Master, {worker_msg, Type, Process, Detail})).

start_link(RegName, Opt)->
    gen_server:start_link({local, RegName}, ?MODULE, [RegName, Opt], []).

init([RegName, Opt])->
    process_flag(trap_exit, true),
    Workspace=proplists:get_value(workspace, Opt),
    Master=proplists:get_value(master, Opt),
    file:make_dir(Workspace),
    {ok, #worker_state{
            name=RegName,
            workspace=Workspace,
            master=Master,
            acc=undefined
           }}.

handle_call({alloc, PWorkerName}, {Pid, _}, State=#worker_state{
                                           master=Master,
                                           name=EWorkerName,
                                           acc=Acc
                                          })->
    Node=node(Pid),
    try
        Task=apply(?task_mod(Master), alloc, [Acc, PWorkerName, Node]),
        case Task of
            none->
                ?send_msg_to_master(Master, 'end', alloc, null);
            {ok, Args}->
                ?send_msg_to_master(Master, ok, alloc, {PWorkerName, Node,
                                                        Args})
        end,
        {reply, Task, State}
    catch
        T:R->
            ?send_msg_to_master(Master, error, alloc,
                                [{eworker, {EWorkerName, node()}},
                                 {pworker, {PWorkerName, Node}},
                                 {what, {T, R}},
                                 ?TRACE
                                ]),
            {reply, {error, {T, R}}, State}
    end;
handle_call(_Msg, _From, State)->
    {reply, {error, "error message"}, State}.

handle_cast({init, Mod, InitArgs}, State=#worker_state{
                                            workspace=Workspace,
                                            master=Master,
                                            name=WorkerName
                                           })->
    Acc=
    try
        case apply(Mod, init, [Workspace, InitArgs]) of
            {ok, A}->
                ?send_msg_to_master(Master, ok, init, WorkerName),
                A;
            {error, Reason}->
                ?send_msg_to_master(Master, error, init, {{WorkerName, node()},
                                                          Reason}),
                undefined
        end
    catch
        T:R->
            ?send_msg_to_master(Master, error, init,
                                [{worker, {WorkerName, node()}},
                                 {what, {T, R}},
                                 ?TRACE
                                ])
    end,
    {noreply, State#worker_state{acc=Acc}};
handle_cast({reduce, Mod}, State=#worker_state{
                                    workspace=Workspace,
                                    master=Master,
                                    name=WorkerName,
                                    acc=Acc
                                   })->
    try
        case apply(Mod, reduce, [Workspace, Acc]) of
            {ok, Result}->
                ?send_msg_to_master(Master, ok, reduce,
                                    {WorkerName, Result});
            {error, Reason}->
                ?send_msg_to_master(Master, error, reduce,
                                    {{WorkerName, node()}, Reason})
        end
    catch
        T:R->
            ?send_msg_to_master(Master, error, reduce,
                                [{worker, {WorkerName, node()}},
                                 {what, {T, R}},
                                 ?TRACE
                                ])
    end,
    {noreply, State};
handle_cast(map, State=#worker_state{
                          workspace=Workspace,
                          master=Master,
                          name=WorkerName
                         })->
    MainWorker=?task_main_worker(Master),
    case gen_server:call(MainWorker, {alloc, WorkerName}) of
        none->
            ?send_msg_to_master(Master, 'end', map, {WorkerName, node()});
        {ok, TaskArgs}->
            Mod=?task_mod(Master),
            try
                Return=apply(Mod, map, [Workspace, TaskArgs]),
                case Return of
                    {ok, Result}->
                        ?send_msg_to_master(Master, ok, map,
                                            {{WorkerName, node()}, TaskArgs,
                                             Result}),
                        gen_server:cast(self(), map);
                    {error, Reason}->
                        ?send_msg_to_master(Master, error, map,
                                            {{WorkerName, node()}, TaskArgs,
                                             Reason})
                end,
                gen_server:cast(MainWorker, {accumulate, WorkerName, node(),
                                             TaskArgs, Return})
            catch
                T:R->
                    ?send_msg_to_master(Master, error, map,
                                        [{worker, {WorkerName, node()}},
                                         {task_args, TaskArgs},
                                         {what, {T, R}},
                                         ?TRACE
                                        ])
            end;
        {error, _Reason}->ok
    end,
    {noreply, State};
handle_cast({accumulate, PWorkerName, Node, TaskArgs, Return},
            State=#worker_state{
                     master=Master,
                     workspace=Workspace,
                     name=EWorkerName,
                     acc=Acc
                    })->
    Mod=?task_mod(Master),
    NewAcc=try
               case apply(Mod, accumulate, [Workspace, Acc, PWorkerName, Node,
                                            TaskArgs, Return]) of
                   {ok, NA}->
                       ?send_msg_to_master(Master, ok, accumulate,
                                           {PWorkerName, Node}),
                       NA;
                   {error, Reason}->
                       ?send_msg_to_master(Master, error, accumulate,
                                           {{EWorkerName, node()},
                                            {PWorkerName, Node},
                                            Return,
                                            Reason}),
                       Acc
               end
           catch
               T:R->
                   ?send_msg_to_master(Master, error, accumulate,
                                       [{eworker, {EWorkerName, node()}},
                                        {pworker, {PWorkerName, Node}},
                                        {return, Return},
                                        {what, {T, R}},
                                        ?TRACE
                                       ]),
                   Acc
           end,
    {noreply, State#worker_state{acc=NewAcc}};
handle_cast(_Msg, State)->
    {noreply, State}.

handle_info(_Msg, State)->
    {noreply, State}.

code_change(_Vsn, State, _Extra)->
    {ok, State}.

terminate(_Reason, _State)->
    ok.
