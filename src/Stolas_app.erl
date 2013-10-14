%% @author Mochi Media <dev@mochimedia.com>
%% @copyright Stolas Mochi Media <dev@mochimedia.com>

%% @doc Callbacks for the Stolas application.

-module(Stolas_app).
-author("Mochi Media <dev@mochimedia.com>").

-behaviour(application).
-export([start/2,stop/1]).


%% @spec start(_Type, _StartArgs) -> ServerRet
%% @doc application start callback for Stolas.
start(_Type, _StartArgs) ->
    Stolas_deps:ensure(),
    Stolas_sup:start_link().

%% @spec stop(_State) -> ServerRet
%% @doc application stop callback for Stolas.
stop(_State) ->
    ok.
