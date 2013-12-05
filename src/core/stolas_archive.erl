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
          config::list(tuple()),
          last_syne_time::{integer(), integer(), integer()}|nil,
          status::wait_master|ok|no_master,
          role::master|sub
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
    case proplists:lookup(master_node, Conf) of
        none->{stop, "master node was not define"};
        {master, MasterNode}->
            Nodes=proplists:get_value(nodes, Conf, [node()]),
            {ok, PingTref}=?ping_tref(Nodes),

            case proplists:get_value(ssh_files_transport, Conf, false) of
                false->ssh:stop();
                true->ssh:start()
            end,

            if
                MasterNode=:=node()->
                    {ok, #archive{
                            task_dict=?dict_new('stolas:task_dict'),
                            ping_tref=PingTref,
                            master_node=MasterNode,
                            config=Conf,
                            last_syne_time=nil,
                            status=ok,
                            role=master
                           }};
                true->
                    gen_server:cast(self(), wait_master),
                    {ok, #archive{
                            last_syne_time={0, 0, 0},
                            status=wait_master,
                            role=sub
                           }}
            end
    end.


handle_call(_Msg, _From, Archive)->
    {reply, {error, "error message"}, Archive}.


handle_cast(wait_master, Archive=#archive{
                                    master_node=MasterNode,
                                    status=wait_master
                                   }) when MasterNode=/=node()->
    try
        {Timestamp, #archive{
                       task_dict=TaskDict,
                       config=Conf
                      }}=
        gen_server:call(get_master_archive, {stolas_archive, MasterNode}),
        {noreply, Archive#archive{
                    task_dict=TaskDict,
                    config=Conf,
                    last_syne_time=Timestamp,
                    status=ok
                   }}
    catch
        exit:{noproc, _}->
            timer:apply_after(1000, gen_server, cast, [self(), wait_master]),
            {noreply, Archive};
        _:_->
            {noreply, Archive#archive{status=no_master}}
    end;
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
