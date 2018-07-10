#!/bin/bash
date
python3.6 CheckPerfsonarIndexing.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem checking ps indexing. Exiting."
    exit $rc
fi