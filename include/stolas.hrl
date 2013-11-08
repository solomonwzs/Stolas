-ifndef(STOLAS_INCLUDED).
-define(STOLAS_INCLUDED, 1).

-record(task_failure_msg, {
          worker::{atom(), atom()},
          mfc::{atom(), atom(), list()},
          reason::atom(),
          msg::term()
         }).

-record(worker_msg, {
          type::ok|error|'end',
          process::init|alloc|map|accumulate|reduce,
          detail::term()
         }).

-define(dict_new(Name), dict:new()).
-define(dict_add(Dict, Key, Value), dict:store(Key, Value, Dict)).
-define(dict_del(Dict, Key), dict:erase(Key, Dict)).
-define(dict_find(Dict, Key), dict:find(Key, Dict)).
-define(dict_drop(Dict), ok).
-define(dict_size(Dict), dict:size(Dict)).

-define(task_id(Task, Type),
        list_to_atom(lists:concat(["stolas_task:", Task, ":", Type]))).

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

-endif.
