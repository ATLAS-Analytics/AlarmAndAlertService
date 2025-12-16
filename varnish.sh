#!/bin/bash
date
python -m ensurepip --upgrade

python3 -m pip install -r requirements.txt
python3 varnish.py > varnish.log 2>&1
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running Varnish alarm. Exiting."
    exit $rc
fi