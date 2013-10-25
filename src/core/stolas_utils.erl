-module(stolas_utils).

-export([get_config/0, get_config/1]).
-export([readable_datetime/1]).

-define(DEFAULT_CONFIG_FILE, "./stolas.config").

readable_datetime(Date)->
    {{Y, M, D}, {H, MM, S}}=Date,
    lists:flatten(io_lib:format(
                    "~4.10.0b-~2.10.0b-~2.10.0b ~2.10.0b:~2.10.0b:~2.10.0b",
                    [Y, M, D, H, MM, S])).

get_config()->
    try
        gen_server:call(stolas_manager, get_config, 500)
    catch
        _:_->
            get_config(define)
    end.
get_config(default)->
    get_config(?DEFAULT_CONFIG_FILE);
get_config(ConfFile)->
    file:consult(ConfFile).
