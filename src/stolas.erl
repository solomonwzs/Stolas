%% @author Mochi Media <dev@mochimedia.com>
%% @copyright 2010 Mochi Media <dev@mochimedia.com>

%% @doc stolas.

-module(stolas).
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
%% @doc Start the stolas server.
start() ->
    stolas_deps:ensure(),
    ensure_started(crypto),
    application:start(stolas).


%% @spec stop() -> ok
%% @doc Stop the stolas server.
stop() ->
    application:stop(stolas).
