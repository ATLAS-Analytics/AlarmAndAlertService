#!/bin/bash
date
# service sendmail start
python3 ps-indexing.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem checking ps indexing. Exiting."
    exit $rc
fi