#!/bin/bash
date
python3.6 CheckClusterState.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem checking cluster state. Exiting."
    exit $rc
fi