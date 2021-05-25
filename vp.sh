#!/bin/bash
date
python3.8 vp.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running VP alarm. Exiting."
    exit $rc
fi