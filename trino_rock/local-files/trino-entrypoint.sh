#!/bin/bash

launcher stop
launcher start

while [ ! -f /data/trino/var/log/server.log ]; do
    sleep 1
done
tail -f /data/trino/var/log/server.log
