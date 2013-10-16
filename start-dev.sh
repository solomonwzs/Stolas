#!/bin/sh
# NOTE: mustache templates need \ because they are not awesome.
exec erl -pa ebin edit deps/*/ebin -boot start_sasl \
    -sname stolas_dev \
    -s stolas \
    -config sys.config \
    -s reloader
