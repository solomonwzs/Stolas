-module(stolas_worker).
-behaviour(gen_server).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-record(worker_state, {
          mod::atom(),
          name::atom(),
          workspace::string(),
          master::atom()|{atom(), atom()}
         }).

start_link(RegName, Opt)->
    gen_server:start_link({local, RegName}, ?MODULE, [RegName, Opt], []).

init([RegName, Opt])->
    process_flag(trap_exit, true),
    Workspace=proplists:get_value(workspace, Opt),
    Mod=proplists:get_value(mod, Opt),
    Master=proplists:get_value(master, Opt),
    file:make_dir(Workspace),
    {ok, #worker_state{
            mod=Mod,
            name=RegName,
            workspace=Workspace,
            master=Master
           }}.

handle_call(_Msg, _From, State)->
    {reply, {error, "error message"}, State}.

handle_cast({init, InitArgs}, State=#worker_state{
                                       mod=Mod,
                                       workspace=Workspace,
                                       master=Master,
                                       name=WorkerName
                                      })->
    Feedback=try
                 case apply(Mod, init, [Workspace, InitArgs]) of
                     ok->
                         {init_complete, {ok, WorkerName}};
                     {error, Reason}->
                         {init_complete, {failed, WorkerName, Reason}}
                 end
             catch
                 T:R->{init_complete, {failed, WorkerName, {T, R}}}
             end,
    gen_server:cast(Master, Feedback),
    {noreply, State};
handle_cast(reduce, State=#worker_state{
                             mod=Mod,
                             workspace=Workspace,
                             master=Master,
                             name=WorkerName
                            })->
    Feedback=try
                 case apply(Mod, reduce, [Workspace]) of
                     {ok, Result}->
                         {reduce_complete, {ok, WorkerName}};
                     {error, Reason}->
                         {reduce_complete, {failed, WorkerName, Reason}}
                 end
             catch
                 T:R->{reduce_complete, {failed, WorkerName, {T, R}}}
             end,
    gen_server:cast(Master, Feedback),
    {noreply, State};
handle_cast(start_task, State)->
    gen_server:cast(self(), get_task),
    {noreply, State};
handle_cast(get_task, State=#worker_state{
                               mod=Mod,
                               workspace=Workspace,
                               master=Master,
                               name=WorkerName
                              })->
    Name={WorkerName, node()},
    case gen_server:call(Master, alloc) of
        none->gen_server:cast(Master, {reduce, {ok, Name}});
        {ok, TaskArgs}->
            try
                case apply(Mod, map, [Workspace, TaskArgs]) of
                    {ok, Result}->
                        gen_server:cast(Master, {map_ok, Name, TaskArgs,
                                                 Result}),
                        gen_server:cast(self(), get_task);
                    {error, Reason}->
                        gen_server:cast(Master, {map_error, Name, Reason})
                end
            catch
                T:R->gen_server:cast(Master, {map_error, Name, {T, R}})
            end
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
