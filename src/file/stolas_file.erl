-module(stolas_file).
-export([upload_module/2]).
-export([upload_file/3, download_file/3]).

-define(MAX_BYTES, 10240).
-define(RPC_TIMEOUT, 1500).

upload_module(Mod, Node)->
    Source=code:which(Mod),
    Dest=filename:join(get_work_path(Node),
                       lists:concat(["ebin/", Mod, ".beam"])),
    upload_file(Source, Node, Dest).

upload_file(Local, Node, Remote)->
    copy_file(Local, Node, Remote, upload).

download_file(Node, Remote, Local)->
    copy_file(Local, Node, Remote, download).

copy_file(Local, Node, Remote, Type)->
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

get_work_path(Node)->
    case rpc:call(Node, file, get_cwd, [], ?RPC_TIMEOUT) of
        {ok, Path}->Path;
        {error, Reason}->error(Reason)
    end.
