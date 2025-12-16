#!/bin/bash
date
python3 -m ensurepip --upgrade
python3 -m pip install -r requirements.txt
python3 top-users-AlarmJIRA.py > top-users.log 2>&1
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem getting top users to JIRA. Exiting."
    exit $rc
fi