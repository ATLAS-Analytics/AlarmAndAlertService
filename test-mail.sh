#!/bin/bash
date
python3.8 test-mail.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem checking cluster state. Exiting."
    exit $rc
fi