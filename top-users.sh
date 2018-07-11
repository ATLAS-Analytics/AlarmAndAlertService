#!/bin/bash
date
python3 top-users-Alarm.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem getting top users. Exiting."
    exit $rc
fi

python3 top-users-AlarmJIRA.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem getting top users to JIRA. Exiting."
    exit $rc
fi