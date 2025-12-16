#!/bin/bash
date
echo 'HEP spec things...'
python3 -m ensurepip --upgrade
python3 -m pip install -r requirements.txt
python3 hepspec.py > hepspec.log 2>&1
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running HEP spec things. Exiting."
    exit $rc
fi