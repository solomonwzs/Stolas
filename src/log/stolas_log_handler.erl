-module(stolas_log_handler).
-behaviour(gen_event).

-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2,
         code_change/3]).
-export([format/2]).

-define(date_now, stolas_utils:readable_datetime(calendar:local_time())).

-define(list_element_format(List),
        case lists:foldr(fun(X, Acc)->
                                 [$,, term_format(X)|Acc]
                         end, [], List) of
            []->[];
            R->tl(R)
        end).

-record(handler_state, {
          log_dev,
          format::default|json
         }).


init([RLogConf])->
    process_flag(trap_exit, true),
    LogFilePath=proplists:get_value(file, RLogConf, "./stolas_log"),
    Format=proplists:get_value(format, RLogConf, default),
    {ok, LogDev}=file:open(LogFilePath, [append]),
    {ok, #handler_state{log_dev=LogDev, format=Format}}.


handle_event({Type, _Gleader, {_Pid, Format, Data}}, State)
  when Type=:=error orelse Type=:=waring_msg orelse Type=:=info_msg->
    file:write(State#handler_state.log_dev,
               format("~s [~p] "++Format++"~n", [?date_now, Type|Data])),
    {ok, State};
handle_event({Type, _Gleader, {_Pid, _, Data}}, State=#handler_state{
                                                         log_dev=LogDev,
                                                         format=Format
                                                        })->
    if
        Format=:=json->
            file:write(LogDev,
                       io_lib:format("~s [~p] ~s~n", 
                                     [?date_now,
                                      Type,
                                      stolas_utils:json_encode(Data)]));
        true->
            file:write(LogDev,
                       format("~s [~p] ~p~n", [?date_now, Type, Data]))
    end,
    {ok, State}.


handle_call(_Query, State)->
    {ok, {error, "bad query"}, State}.


handle_info(_Info, State)->
    {ok, State}.


terminate(_Reason, State)->
    file:close(State#handler_state.log_dev),
    ok.


code_change(_Vsn, State, _Extra)->
    {ok, State}.


-spec term_format(term())->io_lib:chars().
term_format(Term) when is_list(Term)->
    case io_lib:printable_latin1_list(Term) of
        true->io_lib:format("\"~s\"", [Term]);
        false->
            [$[, ?list_element_format(Term), $]]
    end;
term_format(Term) when is_tuple(Term)->
    [${, ?list_element_format(tuple_to_list(Term)), $}];
term_format(Term)->
    io_lib:format("~p", [Term]).


-spec term_format(string(), term())->io_lib:chars().
term_format(CS, Term)->
    case lists:last(CS) of
        $p->term_format(Term);
        _->io_lib:format(CS, [Term])
    end.


-spec get_cs(string(), string())->{string(), string()}.
get_cs([H|T], CS)->
    case lists:member(H, [$c, $f, $e, $g, $s, $w, $p, $W, $P, $B, $X, $#, $b,
                          $x, $+, $n, $i]) of
        true->{lists:reverse([H|CS]), T};
        false->get_cs(T, [H|CS])
    end.


-spec format(string(), list())->io_lib:chars().
format([], _)->
    [];
format([$~|FTail1], TermList)->
    {CS, FTail2}=get_cs(FTail1, [$~]),
    if
        CS=:="~n"->[$\n, format(FTail2, TermList)];
        true->
            [Term|TTail]=TermList,
            [term_format(CS, Term)|format(FTail2, TTail)]
    end;
format([F|FTail], TermList)->
    [F|format(FTail, TermList)].
