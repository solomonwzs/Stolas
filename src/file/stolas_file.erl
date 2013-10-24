-module(stolas_file).
-export([upload_module/2]).

-define(MAX_BYTES, 10240).
-define(RPC_TIMEOUT, 1500).

upload_module(Mod, Node)->
    try
        {ok, Conf}=stolas_utils:get_config(),
        SSHConf=proplists:get_value(ssh, Conf),

        {User, Host, Port}=proplists:get_value(Node, SSHConf),
        SFile=code:which(Mod),
        DFile=filename:join(get_work_path(Node),
            lists:concat(["ebin/", Mod, ".beam"])),

        Conn=case ssh:connect(Host, Port, [{user, User},
                    {silently_accept_hosts, true}]) of
            {ok, C1}->C1;
            {error, Reason1}->error({"ssh connect failure", Reason1})
        end,
        Channel=case ssh_sftp:start_channel(Conn) of
            {ok, C2}->C2;
            {error, Reason2}->
                ssh:close(Conn),
                error({"start ssh channel failure", Reason2})
        end,

        {ok, SFileDev}=file:open(SFile, [read]),
        {ok, DFileDev}=ssh_sftp:open(Channel, DFile, [write, binary]),
        write_remote_file({SFileDev, Channel, DFileDev}),

        file:close(SFileDev),
        ssh_sftp:close(Channel, DFileDev),
        ssh_sftp:stop_channel(Channel),
        ssh:close(Conn)
    catch
        _:W->
            io:format("~p~n", [erlang:get_stacktrace()]),
            {error, W}
    end.

write_remote_file(Arg={SFileDev, Channel, DFileDev})->
    case file:read(SFileDev, ?MAX_BYTES) of
        {ok, Data}->
            ssh_sftp:write(Channel, DFileDev, Data),
            write_remote_file(Arg);
        eof->ok
    end.

get_work_path(Node)->
    case rpc:call(Node, file, get_cwd, [], ?RPC_TIMEOUT) of
        {ok, Path}->Path;
        {error, Reason}->error(Reason)
    end.
