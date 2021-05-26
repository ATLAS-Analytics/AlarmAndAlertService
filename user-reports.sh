#!/bin/bash
date
cd Users
python3.8 user-reports.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem checking cluster state. Exiting."
    exit $rc
fi