-module(stolas_task).
-export([start/1, stop/1]).

-callback init(WorkSpace::string(), Args::term())->
    ok|{error, Reason::term()}.
-callback alloc(Node::atom())->{ok, TaskArgs::term()}|none.
-callback map(WorkSpace::string(), TaskArgs::term())->
    {ok, Result::term()}|{error, Reason::term()}.
-callback reduce(WorkSpace::string())->ok|{error, Reason::term()}.

start(Opt)->
    gen_server:call(stolas_manager, {new_task, Opt}).

stop(Task)->
    gen_server:call(stolas_manager, {close_task, Task, force}).
