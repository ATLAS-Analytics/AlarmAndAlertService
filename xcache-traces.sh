#!/bin/bash
date

python3 xcache-traces.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem checking for bad frontier sql. Exiting."
    exit $rc
fi