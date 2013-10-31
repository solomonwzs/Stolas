-ifndef(STOLAS_INCLUDED).
-define(STOLAS_INCLUDED, 1).

-define(task_id(Task, Type),
        list_to_atom(lists:concat(["stolas_task:", Task, ":", Type]))).

-record(task_failure_msg, {
          task::atom(),
          role::atom(),
          reason::atom(),
          msg::term()
         }).

-endif.
