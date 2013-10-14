-module(stolas_server).
-behaviour(gen_server).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
        code_change/3, terminate/2]).

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
        thread_num::integer()
    }).

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
            task=Task
        }};
init([worker, Mod, WorkSpace, Master])->
    process_flag(trap_exit, true),
    gen_server:cast(self(), work),
    {ok, #worker_state{
            mod=Mod,
            workspace=WorkSpace,
            master=Master
        }}.
handle_call({new_task, Task}, _From, State)
        when is_record(State, manager_state)->
    TaskSets=State#manager_state.task_sets,
    case sets:is_element(Task, TaskSets) of
        true->
            {reply, {error, "already existed"}, State};
        false->
            {reply, ok, State#manager_state{
                    task_sets=sets:add_element(Task, TaskSets)}}
    end;
handle_call(_Msg, _From, State)->
    {reply, reply, State}.

handle_cast(work, State) when is_record(State, worker_state)->
    apply(State#worker_state.mod, work, [State#worker_state.workspace]),
    gen_server:cast(State#worker_state.master, finish),
    {noreply, State};
handle_cast(finish, State) when is_record(State, master_state)->
    Remain=State#master_state.thread_num,
    if
        Remain-1=:=0->
            apply(State#master_state.mod, finish,
                [State#master_state.workspace]),
            gen_server:cast(stolas_manager,
                {close_task, State#master_state.task}),
            {noreply, State};
        true->
            {noreply, State#master_state{thread_num=Remain-1}}
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
