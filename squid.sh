#!/bin/bash
date
python3 squid.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running Squid alarm. Exiting."
    exit $rc
fi