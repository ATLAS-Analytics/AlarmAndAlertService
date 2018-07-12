#!/bin/bash
date
# service sendmail start
python3 frontier-failed-q.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem checking cluster state. Exiting."
    exit $rc
fi