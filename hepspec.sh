#!/bin/bash
date
echo 'HEP spec things...'
python3 hepspec.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running HEP spec things. Exiting."
    exit $rc
fi