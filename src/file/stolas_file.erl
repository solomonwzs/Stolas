-module(stolas_file).

-include("stolas.hrl").

-export([get_work_path/1]).
-export([distribute/2, get_resources/2]).
-export([upload_file/2, download_file/2]).

-define(PDK_FD_LIST, {stolas_file, 16#00}).

-define(MAX_BYTES, 10240).
-define(RPC_TIMEOUT, 1500).

-define(make_dir(Node, Name), rpc:call(Node, os, cmd, ["mkdir -p "++Name],
                                       ?RPC_TIMEOUT)).

-define(new_fd_list, put(?PDK_FD_LIST, [])).
-define(add_fd(CloseFunc, Fd),
        put(?PDK_FD_LIST, [{CloseFunc, Fd}|get(?PDK_FD_LIST)])).
-define(close_fd_list,
        [apply(CloseFunc, Fd)||{CloseFunc, Fd}<-get(?PDK_FD_LIST)]).


-spec upload_file(Source::string(), Dest::{Remote::string(), Node::atom()})->
    ok|{error, Reason::term()}.
upload_file(Source, Dest)->
    copy_file(Source, Dest, upload).


-spec download_file(Source::{Remote::string(), Node::atom()}, Dest::string())->
    ok|{error, Reason::term()}.
download_file(Source, Dest)->
    copy_file(Dest, Source, download).


-spec copy_file(Local::string(), {Remote::string(), Node::atom()},
                upload|download)->
    ok|{error, Reason::term()}.
copy_file(Local, {Remote, Node}, Type)->
    {LocalMode, RemoteMode}=
    if
        Type=:=upload->{[read], [write, binary]};
        Type=:=download->{[write, binary], [read]}
    end,

    ?new_fd_list,
    try
        LocalDev=
        case file:open(Local, LocalMode) of
            {ok, L}->L;
            {error, Reason0}->
                error({"can not open local file", Reason0})
        end,
        ?add_fd(fun file:close/1, [LocalDev]),

        {ok, Conf}=stolas_utils:get_config(),
        SSHConf=proplists:get_value(ssh_nodes_args, Conf),
        {User, Host, Port}=proplists:get_value(Node, SSHConf),

        Conn=
        case ssh:connect(Host, Port,
                         [{user, User},
                          {silently_accept_hosts, true}]) of
            {ok, C1}->C1;
            {error, Reason1}->
                error({"ssh connect failure", Reason1})
        end,
        ?add_fd(fun ssh:close/1, [Conn]),

        Channel=
        case ssh_sftp:start_channel(Conn) of
            {ok, C2}->C2;
            {error, Reason2}->
                error({"start ssh channel failure", Reason2})
        end,
        ?add_fd(fun ssh_sftp:stop_channel/1, [Channel]),

        RemoteHandle=
        case ssh_sftp:open(Channel, Remote, RemoteMode) of
            {ok, H}->H;
            {error, Reason3}->
                error({"can not open remote file", Reason3})
        end,
        ?add_fd(fun ssh_sftp:close/2, [Channel, RemoteHandle]),

        if
            Type=:=upload->
                write_remote_file({LocalDev, Channel, RemoteHandle});
            Type=:=download->
                read_remote_file({LocalDev, Channel, RemoteHandle})
        end
    catch
        _:R->{error, R}
    after
        ?close_fd_list
    end.


-spec write_remote_file({LocalDev::file:fd(), Channel::pid(),
                         RemoteHandle::term()})->
    ok.
write_remote_file(Arg={LocalDev, Channel, RemoteHandle})->
    case file:read(LocalDev, ?MAX_BYTES) of
        {ok, Data}->
            ssh_sftp:write(Channel, RemoteHandle, Data),
            write_remote_file(Arg);
        eof->ok
    end.


-spec read_remote_file({LocalDev::file:fd(), Channel::pid(),
                        RemoteHandle::term()})->
    ok.
read_remote_file(Arg={LocalDev, Channel, RemoteHandle})->
    case ssh_sftp:read(Channel, RemoteHandle, ?MAX_BYTES) of
        {ok, Data}->
            file:write(LocalDev, Data),
            read_remote_file(Arg);
        eof->ok
    end.


-spec get_work_path(Node::atom())->
    {ok, Path::string()}|{error, Reason::term()}.
get_work_path(Node)->
    case rpc:call(Node, file, get_cwd, [], ?RPC_TIMEOUT) of
        OK={ok, _}->OK;
        {error, Reason}->error(Reason)
    end.


-spec distribute(Resources::list({Path::string(), Name::string()}),
                 Nodes::list(atom()))->
    ok|{error, Reason::term()}.
distribute(Resources, Nodes)->
    lists:foreach(
      fun(Node)->
              {ok, CWD}=get_work_path(Node),
              lists:foreach(
                fun({Path, Name})->
                        AbsPath=filename:join(CWD, Path),
                        ?make_dir(Node, AbsPath),
                        upload_file(filename:join(Path, Name),
                                    {filename:join(AbsPath, Name), Node})
                end, Resources)
      end, Nodes).


-spec get_resources(Resources::list({Path::string(), Name::string()}),
                    Node::atom())->
    ok|{error, Reason::term()}.
get_resources(Resources, Node)->
    {ok, CWD}=get_work_path(Node),
    get_resources(Resources, Node, CWD).


-spec get_resources(Resources::list({Path::string(), Name::string()}),
                    Node::atom(), CWD::string())->
    ok|{error, Reason::term()}.
get_resources([], _, _)->ok;
get_resources([{Path, Name}|Tail], Node, CWD)->
    AbsPath=filename:join(CWD, Path),
    ?make_dir(node(), Path),
    case download_file({filename:join(AbsPath, Name), Node},
                       File=filename:join(Path, Name)) of
        ok->get_resources(Tail, Node, CWD);
        {error, Err}->{error, {File, Err}}
    end.
