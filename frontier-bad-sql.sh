#!/bin/bash
date
# service sendmail start
python3.8 frontier-bad-sql.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem checking for bad frontier sql. Exiting."
    exit $rc
fi