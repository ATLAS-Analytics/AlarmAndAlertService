#!/bin/bash
date
python3 TopUsersAlarm.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem getting top users. Exiting."
    exit $rc
fi

python3 TopUsersAlarmJIRA.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem getting top users to JIRA. Exiting."
    exit $rc
fi