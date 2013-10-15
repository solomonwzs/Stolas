-module(stolas_task).
-export([start/1, stop/1]).

-callback init(WorkSpace::string(), Args::term())->ok.
-callback map(WorkSpace::string())->ok.
-callback reduce(WorkSpace::string())->ok.

start(Opt)->
    gen_server:call(stolas_manager, {new_task, Opt}).

stop(Name)->
    supervisor:terminate_child(stolas_sup, Name),
    supervisor:delete_child(stolas_sup, Name).
