-module(stolas_task).
-export([start/1, stop/1]).

-callback init(Workspace::string(), Args::term())->
    ok|{error, Reason::term()}.

-callback alloc(Node::atom())->
    {ok, TaskArgs::term()}|none.

-callback map(Workspace::string(), TaskArgs::term())->
    {ok, Result::term()}|{error, Reason::term()}.

-callback accumulate(Workspace::string(), Acc::term(), WorkerName::atom(),
                     Node::atom(), TaskArgs::term(),
                     Return::{ok, Result::term()}|{error, Reason::term()})->
    {ok, NewAcc::term()}|{error, Reaason::term()}.

-callback reduce(Workspace::string(), Acc::term())->
    {ok, Result::term()}|{error, Reason::term()}.

start(Opt)->
    gen_server:call(stolas_manager, {new_task, Opt}).

stop(Task)->
    gen_server:call(stolas_manager, {close_task, Task, force}).
