-module(stolas_utils).

-include("stolas.hrl").

-export([get_config/0, get_config/1]).
-export([readable_datetime/1]).
-export([json_encode/1, json_encode/2]).


-define(bitstring_tail(Bin),
        case size(Bin) of
            0-> <<>>;
            _-> element(2, split_binary(Bin, 1))
        end).

-define(proplist_to_json(Proplist),
        <<${, (?bitstring_tail(
                  lists:foldl(
                    fun({Key, Value}, Acc)->
                            <<Acc/bitstring, $,, (json_encode(Key))/bitstring,
                              $:, (json_encode(Value))/bitstring>>
                    end,
                    <<>>, Proplist)))/bitstring, $}>>).


readable_datetime(Date)->
    {{Y, M, D}, {H, MM, S}}=Date,
    lists:flatten(io_lib:format(
                    "~4.10.0b-~2.10.0b-~2.10.0b ~2.10.0b:~2.10.0b:~2.10.0b",
                    [Y, M, D, H, MM, S])).


get_config()->
    {ok, #archive{
            config=Conf
           }}=gen_server:call(stolas_archive, get_archive),
    {ok, Conf}.
get_config(default)->
    {ok, ConfFile}=application:get_env(stolas, config_file),
    get_config(ConfFile);
get_config(ConfFile)->
    case file:consult(ConfFile) of
        R={ok, _}->R;
        {error, _}->
            Conf=[{nodes, [node()]},
                  {master_node, node()},
                  {readable_file_log, [{file, "./stolas_log"},
                                       {format, default}]}
                 ],
            {ok, Conf}
    end.


is_string([])->true;
is_string([H|T]) when is_integer(H) andalso H>=0 andalso H=<255->
    is_string(T);
is_string(_)->false.


is_proplist([{K, _}]) when is_atom(K)->true;
is_proplist([{K, _}|T]) when is_atom(K) orelse is_list(K)->
    is_proplist(T);
is_proplist(_)->false.


json_encode(T) when T=:=null orelse T=:=true orelse T=:=false->
    <<(atom_to_binary(T, utf8))/bitstring>>;
json_encode(T) when is_atom(T)->
    <<$", (atom_to_binary(T, utf8))/bitstring, $">>;
json_encode(T) when is_integer(T)->
    integer_to_binary(T);
json_encode(T) when is_float(T)->
    float_to_binary(T);
json_encode(T) when is_binary(T)->
    <<$", T/bitstring, $">>;
json_encode(T) when is_list(T)->
    case is_string(T) of
        true-> <<$", (list_to_binary(T))/bitstring, $">>;
        false->
            case is_proplist(T) of
                true->?proplist_to_json(T);
                false->
                    F=fun(X, Acc)->
                              <<Acc/bitstring, $,, (json_encode(X))/bitstring>>
                      end,
                    Bin=lists:foldl(F, <<>>, T),
                    <<$[, (?bitstring_tail(Bin))/bitstring, $]>>
            end
    end;
json_encode(T) when is_tuple(T)->
    json_encode(tuple_to_list(T));
json_encode(T) when is_pid(T)->
    json_encode(pid_to_list(T));
json_encode(T) when is_port(T)->
    json_encode(erlang:port_to_list(T)).


json_encode(T, dict)->
    Proplist=dict:to_list(T),
    ?proplist_to_json(Proplist);
json_encode(T, gb_tree)->
    Proplist=gb_trees:to_list(T),
    ?proplist_to_json(Proplist).
