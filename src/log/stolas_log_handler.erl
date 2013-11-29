-module(stolas_log_handler).
-behaviour(gen_event).

-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2,
         code_change/3]).

-define(date_now, stolas_utils:readable_datetime(calendar:local_time())).

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
               io_lib:format("~s [~p] "++Format++"~n", [?date_now, Type|Data])),
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
                       io_lib:format("~s [~p] ~p~n", [?date_now, Type, Data]))
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
