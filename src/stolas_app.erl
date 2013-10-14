%% @author Mochi Media <dev@mochimedia.com>
%% @copyright stolas Mochi Media <dev@mochimedia.com>

%% @doc Callbacks for the stolas application.

-module(stolas_app).
-author("Mochi Media <dev@mochimedia.com>").

-behaviour(application).
-export([start/2,stop/1]).


%% @spec start(_Type, _StartArgs) -> ServerRet
%% @doc application start callback for stolas.
start(_Type, _StartArgs) ->
    stolas_deps:ensure(),
    stolas_sup:start_link().

%% @spec stop(_State) -> ServerRet
%% @doc application stop callback for stolas.
stop(_State) ->
    ok.
