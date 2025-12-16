#!/bin/bash
date
echo 'checks number of xcache connections on MWT2 dcache servers...'
python3 -m ensurepip --upgrade
python3 -m pip install -r requirements.txt
python3 xcache.py > xcache.log 2>&1
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running XCache alarm. Exiting."
    exit $rc
fi