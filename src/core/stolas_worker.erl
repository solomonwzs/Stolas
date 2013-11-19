-module(stolas_worker).
-behaviour(gen_server).

-include("stolas.hrl").

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-record(worker_state, {
          name::atom(),
          workspace::string(),
          master::atom(),
          acc::undefined|term()
         }).

-define(task_mod(Master), gen_server:call(Master, get_mod)).
-define(task_main_worker(Master), gen_server:call(Master, get_main_worker)).
-define(send_msg_to_master(Master, Type, Process, Detail),
        gen_server:cast(Master, #worker_msg{
                                   type=Type,
                                   process=Process,
                                   detail=Detail
                                  })).

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

handle_call({alloc, WorkerName}, {Pid, _}, State=#worker_state{
                                           master=Master
                                          })->
    try
        Node=node(Pid),
        Task=apply(?task_mod(Master), alloc, [Node]),
        case Task of
            none->
                %gen_server:cast(Master, alloc_end);
                ?send_msg_to_master(Master, 'end', alloc, null);
            {ok, Args}->
                %gen_server:cast(Master, {new_alloc, WorkerName, Node, Args})
                ?send_msg_to_master(Master, ok, alloc, {WorkerName, Node,
                                                        Args})
        end,
        {reply, Task, State}
    catch
        T:R->
            %gen_server:cast(Master, {alloc_error, WorkerName, {T, R}}),
            ?send_msg_to_master(Master, error, alloc, {WorkerName, {T, R}}),
            {reply, {error, {T, R}}, State}
    end;
handle_call(_Msg, _From, State)->
    {reply, {error, "error message"}, State}.

handle_cast({init, Mod, InitArgs, Acc}, State=#worker_state{
                                                 workspace=Workspace,
                                                 master=Master,
                                                 name=WorkerName
                                                })->
    try
        case apply(Mod, init, [Workspace, InitArgs]) of
            ok->
                %{init_ok, WorkerName};
                ?send_msg_to_master(Master, ok, init, WorkerName);
            {error, Reason}->
                %{init_error, WorkerName, Reason}
                ?send_msg_to_master(Master, error, init, {WorkerName, Reason})
        end
    catch
        T:R->
            %{init_error, WorkerName, {T, R}}
            ?send_msg_to_master(Master, error, init, {WorkerName, {T, R}})
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
                %{reduce_ok, WorkerName, Result};
                ?send_msg_to_master(Master, ok, reduce,
                                    {WorkerName, Result});
            {error, Reason}->
                %{reduce_error, WorkerName, Reason}
                ?send_msg_to_master(Master, error, reduce,
                                    {WorkerName, Reason})
        end
    catch
        T:R->
            %{reduce_error, WorkerName, {T, R}}
            ?send_msg_to_master(Master, error, reduce,
                                {WorkerName, {T, R}})
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
                                        {{WorkerName, node()}, TaskArgs,
                                         {T, R}})
            end;
        {error, _Reason}->ok
    end,
    {noreply, State};
handle_cast({accumulate, WorkerName, Node, TaskArgs, Return},
            State=#worker_state{
                     master=Master,
                     workspace=Workspace,
                     acc=Acc
                    })->
    Mod=?task_mod(Master),
    NewAcc=try
               case apply(Mod, accumulate, [Workspace, Acc, WorkerName, Node,
                                            TaskArgs, Return]) of
                   {ok, NA}->
                       ?send_msg_to_master(Master, ok, accumulate,
                                           {WorkerName, Node}),
                       NA;
                   {error, Reason}->
                       ?send_msg_to_master(Master, error, accumulate,
                                           {WorkerName, Node, Reason}),
                       Acc
               end
           catch
               T:R->
                   ?send_msg_to_master(Master, error, accumulate,
                                       {WorkerName, Node, {T, R}}),
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
