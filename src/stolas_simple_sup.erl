-module(stolas_simple_sup).
-behaviour(supervisor).
-export([start_link/1, start_link/2]).
-export([init/1]).

start_link(Name)->
    supervisor:start_link({local, Name}, ?MODULE, []).

start_link(Name, ChildSpec)->
    supervisor:start_link({local, Name}, ?MODULE, ChildSpec).

init(ChildSpec) ->
    {ok, {{one_for_one, 5, 10}, ChildSpec}}.
