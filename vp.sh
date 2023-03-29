#!/bin/bash
date
echo 'alarm generated on VP xcache servers missing heartbeats.'
python3 vp.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running VP alarm. Exiting."
    exit $rc
fi