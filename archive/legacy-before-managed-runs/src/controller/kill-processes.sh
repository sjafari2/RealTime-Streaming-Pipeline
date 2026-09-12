#!/bin/bash

# Find parent PIDs (PPID) for zombie processes
ps -eo pid,ppid,stat,cmd | awk '$3 ~ /Z/ {print "Zombie PID:", $1, "Parent PID:", $2, "Command:", $4}'

for ppid in $(ps -eo ppid,stat | awk '$2 ~ /Z/ {print $1}' | sort -u | grep -v '^1$'); do
    echo "Killing parent process $ppid"
    kill -9 "$ppid"
done

