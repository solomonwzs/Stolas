-ifndef(__INCLUDE_STOLAS_HRL).
-define(__INCLUDE_STOLAS_HRL, 1).

-define(dict_new(Name), dict:new()).
-define(dict_add(Dict, Key, Value), dict:store(Key, Value, Dict)).
-define(dict_del(Dict, Key), dict:erase(Key, Dict)).
-define(dict_find(Dict, Key), dict:find(Key, Dict)).
-define(dict_drop(Dict), ok).
-define(dict_size(Dict), dict:size(Dict)).

-define(task_id(Task, Type),
        list_to_atom(lists:concat(["stolas_task:", Task, ":", Type]))).

-define(cast_self_after(Msecs, Msg),
        timer:apply_after(Msecs, gen_server, cast, [self(), Msg])).

-define(debug_log(Info), error_logger:info_report(
                           [{tyep, debug},
                            {file, ?FILE},
                            {line, ?LINE},
                            {info, Info}])).

%-define(dict_new(Name), ets:new(Name, [set, private])).
%-define(dict_add(Dict, Key, Value),
%        apply(
%          fun()->
%                  ets:insert(Dict, {Key, Value}),
%                  Dict
%          end, [])).
%-define(dict_del(Dict, Key),
%        apply(
%          fun()->
%                  ets:delete(Dict, Key),
%                  Dict
%          end, [])).
%-define(dict_find(Dict, Key),
%        apply(
%          fun()->
%                  case ets:lookup(Dict, Key) of
%                      [{Key, Value}]->Value;
%                      _->error
%                  end
%          end, [])).
%-define(dict_drop(Dict),
%        apply(
%          fun()->
%                  ets:delete(Dict),
%                  ok
%          end, [])).
%-define(dict_size(Dict), ets:info(Dict, size)).

-record(archive, {
          task_dict::dict:dict(),
          config::list(tuple()),
          master_node::atom(),
          last_syne_time::{integer(), integer(), integer()}|nil,
          status::wait_master|ok|no_master,
          role::master|sub,
          lock::{pid(), read|write}|nil
         }).

-endif.
