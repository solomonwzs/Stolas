-module(stolas_utils).
-export([readable_datetime/1, lists_split/2]).

readable_datetime(Date)->
    {{Y, M, D}, {H, MM, S}}=Date,
    lists:flatten(io_lib:format(
            "~4.10.0b-~2.10.0b-~2.10.0b ~2.10.0b:~2.10.0b:~2.10.0b",
            [Y, M, D, H, MM, S])).

lists_split(List, N)->
    _SL=round(length(List)/N).
