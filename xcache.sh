#!/bin/bash
date
python3.8 xcache.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running XCache alarm. Exiting."
    exit $rc
fi