#!/bin/bash
date

python3 alerts.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem sending alerts. Exiting."
    exit $rc
fi