-module(stolas_server).
-behaviour(gen_server).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
        code_change/3, terminate/2]).

-define(id(Name, Type), list_to_atom(lists:concat([Name, ":", Type]))).
-define(sub_workspace(WorkSpace, X), filename:join(WorkSpace, X)).
-define(broadcast_map_msg(Task, ThreadNum),
    [gen_server:cast(?id(Task, X), map)||X<-lists:seq(1, ThreadNum)]).

-record(manager_state, {
        task_sets::tuple()
    }).
-record(worker_state, {
        mod::atom(),
        workspace::string(),
        master::atom()
    }).
-record(master_state, {
        task::atom(),
        mod::atom(),
        workspace::string(),
        thread_num::integer(),
        finish_tasks::integer()
    }).
%-record(failure_msg, {
%        task::atom(),
%        msg::term()
%    }).

start_link(RegName, Opt)->
    case proplists:get_value(role, Opt) of
        master->
            WorkSpace=proplists:get_value(workspace, Opt),
            Mod=proplists:get_value(mod, Opt),
            ThreadNum=proplists:get_value(thread_num, Opt),
            Task=proplists:get_value(task, Opt),
            gen_server:start_link({local, RegName}, ?MODULE,
                [master, Mod, WorkSpace, ThreadNum, Task], []);
        manager->
            gen_server:start_link({local, RegName}, ?MODULE, [manager], []);
        worker->
            WorkSpace=proplists:get_value(workspace, Opt),
            Mod=proplists:get_value(mod, Opt),
            Master=proplists:get_value(master, Opt),
            gen_server:start_link({local, RegName}, ?MODULE,
                [worker, Mod, WorkSpace, Master], [])
    end.

init([manager])->
    process_flag(trap_exit, true),
    {ok, #manager_state{task_sets=sets:new()}};
init([master, Mod, WorkSpace, ThreadNum, Task])->
    process_flag(trap_exit, true),
    {ok, #master_state{
            mod=Mod,
            workspace=WorkSpace,
            thread_num=ThreadNum,
            task=Task,
            finish_tasks=0
        }};
init([worker, Mod, WorkSpace, Master])->
    process_flag(trap_exit, true),
    {ok, #worker_state{
            mod=Mod,
            workspace=WorkSpace,
            master=Master
        }}.

handle_call({new_task, Opt}, _From, State)
        when is_record(State, manager_state)->
    TaskSets=State#manager_state.task_sets,
    Task=proplists:get_value(task, Opt),
    case sets:is_element(Task, TaskSets) of
        true->
            {reply, {error, "already existed"}, State};
        false->
            Mod=proplists:get_value(mod, Opt),
            WorkSpace=proplists:get_value(workspace, Opt),
            ThreadNum=proplists:get_value(thread_num, Opt),
            MasterId=?id(Task, master),
            MasterSpec={
                MasterId,
                {stolas_server, start_link,
                    [MasterId, [{role, master}, {thread_num, ThreadNum},
                            {mod, Mod}, {workspace, WorkSpace}, {task, Task}]]},
                permanent,
                5000,
                worker,
                [stolas_server]
            },
            ChildSpecs=[{
                    ?id(Task, X),
                    {stolas_server, start_link,
                        [?id(Task, X), [{role, worker}, {mod, Mod},
                                {master, MasterId},
                                {workspace, 
                                    ?sub_workspace(WorkSpace,
                                        integer_to_list(X))}]]},
                    permanent, 
                    5000,
                    worker,
                    [stolas_server]
                }||X<-lists:seq(1, ThreadNum)],
            Res=supervisor:start_child(stolas_sup, {Task,
                    {stolas_simple_sup, start_link,
                        [Task, [MasterSpec|ChildSpecs]]},
                    permanent, infinity, supervisor, [stolas_simple_sup]}),
            case Res of
                {ok, _}->
                    NewTaskSets=sets:add_element(Task, TaskSets),
                    InitArgs=proplists:get_value(init_args, Opt),
                    gen_server:cast(MasterId, {init, InitArgs}),
                    {reply, Res, State#manager_state{task_sets=NewTaskSets}};
                _->{reply, {error, Res}, State}
            end
    end;
handle_call(_Msg, _From, State)->
    {reply, reply, State}.

handle_cast(map, State) when is_record(State, worker_state)->
    apply(State#worker_state.mod, map, [State#worker_state.workspace]),
    gen_server:cast(State#worker_state.master, reduce),
    {noreply, State};

handle_cast({init, InitArgs}, State) when is_record(State, master_state)->
    #master_state{
        task=Task,
        thread_num=ThreadNum,
        workspace=WorkSpace,
        mod=Mod
    }=State,
    lists:foreach(fun(X)->
                file:make_dir(?sub_workspace(WorkSpace, integer_to_list(X)))
        end, lists:seq(1, ThreadNum)),
    apply(Mod, init, [WorkSpace, InitArgs]),
    ?broadcast_map_msg(Task, ThreadNum),
    {noreply, State};
handle_cast(reduce, State) when is_record(State, master_state)->
    FinishTasks=State#master_state.finish_tasks,
    if
        FinishTasks+1=:=State#master_state.thread_num->
            apply(State#master_state.mod, reduce,
                [State#master_state.workspace]),
            gen_server:cast(stolas_manager,
                {close_task, State#master_state.task}),
            {noreply, State};
        true->
            {noreply, State#master_state{finish_tasks=FinishTasks+1}}
    end;

handle_cast({close_task, Task}, State) when is_record(State, manager_state)->
    TaskSets=State#manager_state.task_sets,
    case sets:is_element(Task, TaskSets) of
        true->
            supervisor:terminate_child(stolas_sup, Task),
            supervisor:delete_child(stolas_sup, Task),
            {noreply, State#manager_state{
                    task_sets=sets:del_element(Task, TaskSets)}};
        false->
            {noreply, State}
    end;

handle_cast(_Msg, State)->
    {noreply, State}.

handle_info(_Msg, State)->
    {noreply, State}.

code_change(_Vsn, State, _Extra)->
    {ok, State}.

terminate(_Reason, _State)->
    ok.
