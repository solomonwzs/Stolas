-module(stolas_task).
-export([start/1, stop/1]).

-callback init(WorkSpace::string(), Args::term())->ok.
-callback map(WorkSpace::string())->ok.
-callback reduce(WorkSpace::string())->ok.

start(Opt)->
    gen_server:call(stolas_manager, {new_task, Opt}).

stop(Task)->
    gen_server:call(stolas_manager, {close_task, Task}).
