-module(stolas_task).
-export([start/1, stop/1]).

-callback init(Workspace::string(), Args::term())->
    {ok, Acc::term()}|{error, Reason::term()}.

-callback alloc(Acc::term(), WorkerName::atom(), Node::atom())->
    {{ok, TaskArgs::term()}|none, NewAcc::term()}.

-callback map(Workspace::string(), TaskArgs::term())->
    {ok, Result::term()}|{error, Reason::term()}.

-callback accumulate(Workspace::string(), Acc::term(), WorkerName::atom(),
                     Node::atom(), TaskArgs::term(),
                     Return::{ok, Result::term()}|{error, Reason1::term()})->
    {ok, NewAcc::term()}|{error, Reason2::term()}.

-callback reduce(Workspace::string(), Acc::term())->
    {ok, Result::term()}|{error, Reason::term()}.


start(Opt)->
    LeaderNode=proplists:get_value(leader_node, Opt, node()),
    gen_server:call({stolas_manager, LeaderNode}, {new_task, Opt, LeaderNode}).


stop(Task)->
    gen_server:call(stolas_manager, {close_task, Task, force}).
