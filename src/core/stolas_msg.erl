-module(stolas_msg).

-export([process_worker_err_msg/2]).

process_worker_err_msg(init, {EWorker, Reason})->
    io_lib:format("init error, executive worker:~p, reason:~p",
                  [EWorker, Reason]);
process_worker_err_msg(alloc, {EWorker, PWorker, Reason})->
    io_lib:format("alloc error, executive worker:~p, passing worker:~p, reason:~p",
                  [EWorker, PWorker, Reason]);
process_worker_err_msg(map, {EWorker, TaskArgs, Reason})->
    io_lib:format("map error, executive worker:~p, task args:~p, reason:~p",
                  [EWorker, TaskArgs, Reason]);
process_worker_err_msg(accumulate, {EWorker, PWorker, Return, Reason})->
    io_lib:format("accumulate error, executive worker:~p, passing worker:~p, return:~p, reason:~p",
                  [EWorker, PWorker, Return, Reason]);
process_worker_err_msg(reduce, {EWorker, Reason})->
    io_lib:format("reduce error, executive worker:~p, reason:~p",
                  [EWorker, Reason]).
