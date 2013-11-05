-module(stolas_worker).
-behaviour(gen_server).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-record(worker_state, {
          name::atom(),
          workspace::string(),
          master::atom()
         }).

-define(task_mod(Master), gen_server:call(get_mod, Master)).
-define(task_main_worker(Master), gen_server:call(get_main_worker, Master)).

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

handle_call(alloc, {Pid, _}, State=#worker_state{
                                           master=Master
                                          })->
    try
        Task=apply(?task_mod(Master), alloc, [node(Pid)]),
        {reply, Task, State}
    catch
        T:R->{reply, {error, {T, R}}}
    end;
handle_call(_Msg, _From, State)->
    {reply, {error, "error message"}, State}.

handle_cast({init, Mod, InitArgs}, State=#worker_state{
                                       workspace=Workspace,
                                       master=Master,
                                       name=WorkerName
                                      })->
    Feedback=try
                 case apply(Mod, init, [Workspace, InitArgs]) of
                     ok->
                         {init_ok, WorkerName};
                     {error, Reason}->
                         {init_error, WorkerName, Reason}
                 end
             catch
                 T:R->{init_error, WorkerName, {T, R}}
             end,
    gen_server:cast(Master, Feedback),
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
    case gen_server:call(MainWorker, alloc) of
        none->gen_server:cast(Master, {reduce, {ok, WorkerName}});
        {ok, TaskArgs}->
            Mod=?task_mod(Master),
            try
                case apply(Mod, map, [Workspace, TaskArgs]) of
                    {ok, Result}->
                        gen_server:cast(Master, {map_ok, {WorkerName, node()},
                                                 TaskArgs, Result}),
                        gen_server:cast(self(), map);
                    {error, Reason}->
                        gen_server:cast(Master, {map_error,
                                                 {WorkerName, node()}, TaskArgs,
                                                 Reason})
                end
            catch
                T:R->gen_server:cast(Master, {map_error, {WorkerName, node()},
                                              TaskArgs, {T, R}})
            end;
        {error, Reason}->
            gen_server:cast(Master, {alloc_error, WorkerName, Reason})
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
