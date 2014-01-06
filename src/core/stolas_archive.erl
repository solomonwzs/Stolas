-module(stolas_archive).
-behaviour(gen_server).

-include("stolas.hrl").

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-define(PING_INTERVAL, 1500).

-define(ping_tref(Nodes),
        timer:apply_interval(
          ?PING_INTERVAL,
          lists, foreach, [fun(Node)->
                                   net_adm:ping(Node)
                           end, Nodes])).
-define(not_lock(Lock), (Lock=:=nil)).
-define(not_wirte_lock(Lock),
        (not (Lock=/=nil andalso element(2, Lock)=:=write))).
-define(match_lock(Lock, Pid, Type),
        (Lock=:={Pid, Type})).


start_link(RegName, Conf)->
    gen_server:start_link({local, RegName}, ?MODULE, [Conf], []).


init([Conf])->
    process_flag(trap_exit, true),
    case proplists:lookup(master_node, Conf) of
        none->{stop, "master node was not defined"};
        {master_node, MasterNode}->
            Archive=
            if
                MasterNode=:=node()->
                    ?cast_self_after(1000, check_nodes),
                    process_conf(Conf),
                    #archive{
                       task_dict=?dict_new('stolas:task_dict'),
                       config=Conf,
                       last_syne_time=nil,
                       status=ok,
                       role=master
                      };
                true->
                    ?cast_self_after(0, wait_master),
                    #archive{
                       last_syne_time={0, 0, 0},
                       status=wait_master,
                       role=sub
                      }
            end,
            {ok, Archive#archive{
                   master_node=MasterNode,
                   lock=nil
                  }}
    end.


handle_call(Msg={get_archive, Pid, write}, _From,
            Archive=#archive{
                       master_node=MasterNode,
                       status=ok,
                       lock=Lock
                      })->
    if
        MasterNode=:=node() andalso ?not_lock(Lock)->
            NewArchive=Archive#archive{lock={Pid, write}},
            {reply, {ok, NewArchive}, NewArchive};
        MasterNode=/=node()->
            case gen_server:call({stolas_archive, MasterNode}, Msg) of
                OK={ok, NewArchive}->
                    {reply, OK, NewArchive};
                Err={error, _}->
                    {reply, Err, Archive}
            end;
        true->{reply, {error, lock}, Archive}
    end;
handle_call(Msg={set_archive, NewArchive, Pid, write}, _From,
            Archive=#archive{
                       master_node=MasterNode,
                       status=ok,
                       lock=Lock
                      })->
    if
        MasterNode=:=node() andalso ?match_lock(Lock, Pid, write)->
            {reply, ok, NewArchive#archive{lock=nil}};
        MasterNode=/=node()->
            case gen_server:call({stolas_archive, MasterNode}, Msg) of
                ok->{reply, ok, NewArchive#archive{last_syne_time=now()}};
                Err={error, _}->{reply, Err, Archive}
            end;
        true->{reply, {error, lock_not_match}, Archive}
    end;
handle_call(get_archive, _From, Archive=#archive{
                                           master_node=MasterNode,
                                           status=ok
                                          })->
    if
        MasterNode=:=node()->{reply, {ok, Archive}, Archive};
        true->
            case sync_archive(Archive) of
                Reply={ok, NewArchive}->{reply, Reply, NewArchive};
                Err={error, _}->{reply, Err, Archive}
            end
    end;
handle_call(get_master_archive, _From, Archive=#archive{
                                                  master_node=MasterNode,
                                                  status=ok
                                                 }) when MasterNode=:=node()->
    {reply, {ok, now(), Archive}, Archive};
handle_call(get_master_archive, _From, Archive=#archive{
                                                  master_node=MasterNode,
                                                  status=ok,
                                                  last_syne_time=LastSyncTime
                                                 }) when MasterNode=/=node()->
    {reply, {master_change, LastSyncTime, MasterNode}, Archive};
handle_call(_Msg, _From, Archive)->
    {reply, {error, error_message}, Archive}.


handle_cast(wait_master, Archive=#archive{
                                    master_node=MasterNode,
                                    status=wait_master
                                   }) when MasterNode=/=node()->
    case get_master_archive(MasterNode) of
        {ok, Timestamp, #archive{
                           task_dict=TaskDict,
                           config=Conf
                          }}->
            ?cast_self_after(1000, check_nodes),
            process_conf(Conf),
            {noreply, Archive#archive{
                        task_dict=TaskDict,
                        config=Conf,
                        last_syne_time=Timestamp,
                        status=ok
                       }};
        {error, Reason} when Reason=:=noreply orelse Reason=:=timeout orelse
                             Reason=:=nodedown->
            ?cast_self_after(1000, wait_master),
            {noreply, Archive};
        {master_change, _, NewMasterNode}->
            ?cast_self_after(1000, wait_master),
            {noreply, Archive#archive{
                        master_node=NewMasterNode
                       }};
        _->{noreply, Archive#archive{status=no_master}}
    end;
handle_cast(check_nodes, Archive=#archive{
                                    config=Conf
                                   })->
    Nodes=proplists:get_value(nodes, Conf, [node()]),
    spawn(lists, foreach, [fun(Node)->net_adm:ping(Node) end, Nodes]),
    ?cast_self_after(1000, check_nodes),
    {noreply, Archive};
handle_cast(_Msg, Archive)->
    {noreply, Archive}.


handle_info(_Msg, Archive)->
    {noreply, Archive}.


code_change(_Vsn, Archive, _Extra)->
    {ok, Archive}.


terminate(_Reason, _Archive)->
    ok.


get_master_archive(MasterNode)->
    try
        gen_server:call({stolas_archive, MasterNode}, get_master_archive, 1000)
    catch
        exit:{noreply, _}->{error, noreply};
        exit:{timeout, _}->{error, timeout};
        exit:{{nodedown, MasterNode}, _}->{error, nodedown};
        _:_->{error, unexpected}
    end.


sync_archive(Archive=#archive{
                        master_node=MasterNode
                       })->
    if
        MasterNode=:=node()->{ok, Archive};
        true->
            case get_master_archive(MasterNode) of
                {ok, Timestamp, #archive{
                                   task_dict=TaskDict,
                                   config=Conf
                                  }}->
                    {ok, Archive#archive{
                           task_dict=TaskDict,
                           config=Conf,
                           last_syne_time=Timestamp
                          }};
                {master_change, _, NewMasterNode}->
                    sync_archive(Archive#archive{master_node=NewMasterNode});
                Err={error, _}->Err
            end
    end.


process_conf(Conf)->
    case proplists:get_value(ssh_files_transport, Conf, false) of
        true->ssh:start();
        _->ssh:stop()
    end,
    case proplists:get_value(readable_file_log, Conf) of
        RLogConf when is_list(RLogConf)->
            error_logger:add_report_handler(stolas_log_handler, [RLogConf]);
        _->ok
    end.
