-module(stolas_archive).
-behaviour(gen_server).

-include("stolas.hrl").

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-record(archive, {
          task_dict::tuple(),
          ping_tref::term(),
          master_node::atom(),
          config::list(tuple())
         }).

-define(PING_INTERVAL, 1500).

-define(ping_tref(Nodes),
        timer:apply_interval(
          ?PING_INTERVAL,
          lists, foreach, [fun(Node)->
                                   net_adm:ping(Node)
                           end, Nodes])).


start_link(RegName, Conf)->
    gen_server:start_link({local, RegName}, ?MODULE, [Conf], []).


init([Conf])->
    process_flag(trap_exit, true),
    {PingTref, MasterNode}=process_conf(Conf),
    {ok, #archive{
            task_dict=?dict_new('stolas:task_dict'),
            ping_tref=PingTref,
            master_node=MasterNode,
            config=Conf
           }}.


handle_call(_Msg, _From, Archive)->
    {reply, {error, "error message"}, Archive}.


handle_cast(_Msg, Archive)->
    {noreply, Archive}.


handle_info(_Msg, Archive)->
    {noreply, Archive}.


code_change(_Vsn, Archive, _Extra)->
    {ok, Archive}.


terminate(_Reason, #archive{
                      ping_tref=PingTref
                     })->
    timer:cancel(PingTref),
    ok.


process_conf(Conf)->
    Nodes=proplists:get_value(nodes, Conf, [node()]),
    {ok, PingTref}=?ping_tref(Nodes),

    case proplists:get_value(ssh_files_transport, Conf, false) of
        true->ssh:start();
        _->ssh:stop()
    end,
    case proplists:get_value(readable_file_log, Conf) of
        RLogConf when is_list(RLogConf)->
            error_logger:add_report_handler(stolas_log_handler, [RLogConf]);
        _->ok
    end,
    MasterNode=proplists:get_value(master_node, Conf),

    {PingTref, MasterNode}.
