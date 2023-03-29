#!/bin/bash
date
echo 'checks number of xcache connections on MWT2 dcache servers...'
python3 xcache.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running XCache alarm. Exiting."
    exit $rc
fi