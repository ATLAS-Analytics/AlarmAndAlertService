#!/bin/bash
date
# service sendmail start
cd Users
python3 user-reports.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem checking cluster state. Exiting."
    exit $rc
fi