#!/bin/bash
date

python3 es-cluster-state.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem checking cluster state. Exiting."
    exit $rc
fi