#!/bin/bash
date
python3 -m ensurepip --upgrade
python3 -m pip install -r requirements.txt
python3 alerts.py > alerts.log 2>&1
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem sending alerts. Exiting."
    exit $rc
fi