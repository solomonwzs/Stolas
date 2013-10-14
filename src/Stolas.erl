%% @author Mochi Media <dev@mochimedia.com>
%% @copyright 2010 Mochi Media <dev@mochimedia.com>

%% @doc Stolas.

-module(Stolas).
-author("Mochi Media <dev@mochimedia.com>").
-export([start/0, stop/0]).

ensure_started(App) ->
    case application:start(App) of
        ok ->
            ok;
        {error, {already_started, App}} ->
            ok
    end.


%% @spec start() -> ok
%% @doc Start the Stolas server.
start() ->
    Stolas_deps:ensure(),
    ensure_started(crypto),
    application:start(Stolas).


%% @spec stop() -> ok
%% @doc Stop the Stolas server.
stop() ->
    application:stop(Stolas).
