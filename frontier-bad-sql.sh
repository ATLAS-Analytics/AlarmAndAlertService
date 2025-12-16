#!/bin/bash
date
python3 -m ensurepip --upgrade
python3 -m pip install -r requirements.txt
python3 frontier-bad-sql.py > frontier-bad-sql.log 2>&1
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem checking for bad frontier sql. Exiting."
    exit $rc
fi