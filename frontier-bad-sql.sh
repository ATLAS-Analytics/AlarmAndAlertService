#!/bin/bash
date

python3 frontier-bad-sql.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem checking for bad frontier sql. Exiting."
    exit $rc
fi