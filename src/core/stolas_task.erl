-module(stolas_task).
-export([start/1, stop/1]).

-callback init(WorkSpace::string(), Args::term())->
    ok|{error, Reason::term()}.
-callback map(WorkSpace::string())->
    {ok, Result::term()}|{error, Reason::term()}.
-callback reduce(WorkSpace::string(), WorkerResults::list())->
    ok|{error, Reason::term()}.

start(Opt)->
    gen_server:call(stolas_manager, {new_task, Opt}).

stop(Task)->
    gen_server:call(stolas_manager, {close_task, Task, force}).
