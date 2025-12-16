#!/bin/bash
date

python3 es-cluster-state.py > es-cluster-state.log 2>&1
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem checking cluster state. Exiting."
    exit $rc
fi