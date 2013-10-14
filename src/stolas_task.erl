-module(stolas_task).
-export([start/1, stop/1]).

-define(id(Name, X), list_to_atom(lists:concat([Name, ":", X]))).
-define(workspace(WorkSpace, X), lists:concat([WorkSpace, "/", X])).

-callback init(WorkSpace::string(), Args::term())->ok.
-callback work(WorkSpace::string())->ok.
-callback finish(WorkSpace::string())->ok.

start(Opt)->
    Name=proplists:get_value(task, Opt),
    case gen_server:call(stolas_manager, {new_task, Name}) of
        ok->
            Mod=proplists:get_value(mod, Opt),
            WorkSpace=proplists:get_value(workspace, Opt),
            ThreadNum=proplists:get_value(thread_num, Opt),
            InitArgs=proplists:get_value(init_args, Opt),
            apply(Mod, init, [WorkSpace, InitArgs]),
            lists:foreach(fun(X)->
                        file:make_dir(?workspace(WorkSpace, X))
                end, lists:seq(1, ThreadNum)),
            MasterId=?id(Name, master),
            MasterSpec={
                MasterId,
                {stolas_server, start_link,
                    [MasterId, [{role, master}, {thread_num, ThreadNum},
                            {mod, Mod}, {workspace, WorkSpace}, {task, Name}]]},
                permanent,
                5000,
                worker,
                [stolas_server]
            },
            ChildSpecs=[{
                    ?id(Name, X),
                    {stolas_server, start_link,
                        [?id(Name, X), [{role, worker}, {mod, Mod},
                                {master, MasterId},
                                {workspace, ?workspace(WorkSpace, X)}]]},
                    permanent, 
                    5000,
                    worker,
                    [stolas_server]
                }||X<-lists:seq(1, ThreadNum)],
            supervisor:start_child(stolas_sup, {Name,
                    {stolas_simple_sup, start_link,
                        [Name, [MasterSpec|ChildSpecs]]},
                    permanent, infinity, supervisor, [stolas_simple_sup]});
        Error->Error
    end.

stop(Name)->
    supervisor:terminate_child(stolas_sup, Name),
    supervisor:delete_child(stolas_sup, Name).
