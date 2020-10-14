#!/bin/bash
date
# service sendmail start
python3.8 es-cluster-state.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem checking cluster state. Exiting."
    exit $rc
fi