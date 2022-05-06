#!/bin/bash
date

# export XRD_LOGLEVEL=Debug

# check for proxy and export it.
export X509_USER_PROXY=/etc/proxy/x509up

while [ ! -f $X509_USER_PROXY ]
do
  sleep 10
  echo "waiting for x509 proxy."
done

ls -lh $X509_USER_PROXY

python3 xcache-traces.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem checking rucio traces. Exiting."
    exit $rc
fi