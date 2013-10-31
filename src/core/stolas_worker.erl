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
    {ok, #worker_state{
            mod=Mod,
            name=RegName,
            workspace=Workspace,
            master=Master
           }}.

handle_call(_Msg, _From, State)->
    {reply, {error, "error message"}, State}.

handle_cast(start_task, State=#worker_state{
                                 workspace=Workspace
                                })->
    file:make_dir(Workspace),
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
