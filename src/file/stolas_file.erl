-module(stolas_file).
-export([get_work_path/1]).
-export([distribute/2, get_resources/2]).
-export([upload_file/2, download_file/2]).

-define(MAX_BYTES, 10240).
-define(RPC_TIMEOUT, 1500).

-define(make_dir(Node, Name), rpc:call(Node, os, cmd, ["mkdir -p "++Name],
                                       ?RPC_TIMEOUT)).

-spec upload_file(Source::string(), Dest::{Remote::string(), Node::atom()})->
    ok|{error, Reason::term()}.
upload_file(Source, Dest)->
    copy_file(Source, Dest, upload).

-spec download_file(Source::{Remote::string(), Node::atom()}, Dest::string())->
    ok|{error, Reason::term()}.
download_file(Source, Dest)->
    copy_file(Dest, Source, download).

copy_file(Local, {Remote, Node}, Type)->
    {LocalMode, RemoteMode}=
    if
        Type=:=upload->{[read], [write, binary]};
        Type=:=download->{[write, binary], [read]}
    end,
    case file:open(Local, LocalMode) of
        {error, Err}->{error, {"can not open local file", Err}};
        {ok, LocalDev}->
            try
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
                Channel=
                case ssh_sftp:start_channel(Conn) of
                    {ok, C2}->C2;
                    {error, Reason2}->
                        ssh:close(Conn),
                        error({"start ssh channel failure", Reason2})
                end,
                RemoteHandle=
                case ssh_sftp:open(Channel, Remote, RemoteMode) of
                    {ok, H}->H;
                    {error, Reason3}->
                        ssh_sftp:stop_channel(Channel),
                        ssh:close(Conn),
                        error({"can not open remote file", Reason3})
                end,

                if
                    Type=:=upload->
                        write_remote_file({LocalDev, Channel, RemoteHandle});
                    Type=:=download->
                        read_remote_file({LocalDev, Channel, RemoteHandle})
                end,

                ssh_sftp:close(Channel, RemoteHandle),
                ssh_sftp:stop_channel(Channel),
                ssh:close(Conn)
            catch
                _:R->{error, R}
            after
                file:close(LocalDev)
            end
    end.

write_remote_file(Arg={LocalDev, Channel, RemoteHandle})->
    case file:read(LocalDev, ?MAX_BYTES) of
        {ok, Data}->
            ssh_sftp:write(Channel, RemoteHandle, Data),
            write_remote_file(Arg);
        eof->ok
    end.

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
        {ok, Path}->Path;
        {error, Reason}->error(Reason)
    end.

-spec distribute(Resources::list({Path::string(), Name::string()}),
                 Nodes::list(atom()))->
    ok|{error, Reason::term()}.
distribute(Resources, Nodes)->
    lists:foreach(
      fun(Node)->
              CWD=get_work_path(Node),
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
    get_resources(Resources, Node, get_work_path(Node)).

get_resources([], _, _)->ok;
get_resources([{Path, Name}|Tail], Node, CWD)->
    AbsPath=filename:join(CWD, Path),
    ?make_dir(node(), Path),
    case download_file({filename:join(AbsPath, Name), Node},
                       File=filename:join(Path, Name)) of
        ok->get_resources(Tail, Node, CWD);
        {error, Err}->{error, {File, Err}}
    end.
