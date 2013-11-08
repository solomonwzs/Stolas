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
          acc::null|term()
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
            master=Master
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

handle_cast({init, Mod, InitArgs}, State=#worker_state{
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
    {noreply, State};
handle_cast({reduce, Mod}, State=#worker_state{
                             workspace=Workspace,
                             master=Master,
                             name=WorkerName
                            })->
    Feedback=try
                 case apply(Mod, reduce, [Workspace]) of
                     {ok, Result}->
                         {reduce_ok, WorkerName, Result};
                     {error, Reason}->
                         {reduce_error, WorkerName, Reason}
                 end
             catch
                 T:R->{reduce_error, WorkerName, {T, R}}
             end,
    gen_server:cast(Master, Feedback),
    {noreply, State};
handle_cast(map, State=#worker_state{
                          workspace=Workspace,
                          master=Master,
                          name=WorkerName
                         })->
    MainWorker=?task_main_worker(Master),
    case gen_server:call(MainWorker, {alloc, WorkerName}) of
        none->gen_server:cast(Master, {work_end, WorkerName});
        {ok, TaskArgs}->
            Mod=?task_mod(Master),
            try
                Return=apply(Mod, map, [Workspace, TaskArgs]),
                case Return of
                    {ok, Result}->
                        gen_server:cast(Master, {map_ok, {WorkerName, node()},
                                                 TaskArgs, Result}),
                        gen_server:cast(self(), map);
                    {error, Reason}->
                        gen_server:cast(Master, {map_error,
                                                 {WorkerName, node()}, TaskArgs,
                                                 Reason})
                end,
                gen_server:cast(MainWorker, {accumulate, WorkerName, node(),
                                             TaskArgs, Return})
            catch
                T:R->gen_server:cast(Master, {map_error, {WorkerName, node()},
                                              TaskArgs, {T, R}})
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
               apply(Mod, accumulate, [Workspace, WorkerName, Node, TaskArgs,
                                       Return])
           catch
               T:R->
                   gen_server:cast(Master, {acc_error, {T, R}}),
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
