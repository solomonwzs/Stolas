-module(stolas_task).
-export([start/1, stop/1]).

-callback init(Workspace::string(), Args::term())->
    ok|{error, Reason::term()}.

-callback alloc(WorkerName::atom(), Node::atom())->
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
    LeaderNode=proplists:get_value(leader_node, Opt, node()),
    gen_server:call({stolas_manager, LeaderNode}, {new_task, Opt, LeaderNode}).


stop(Task)->
    gen_server:call(stolas_manager, {close_task, Task, force}).
