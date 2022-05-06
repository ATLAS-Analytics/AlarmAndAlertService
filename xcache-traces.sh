#!/bin/bash
date

# export XRD_LOGLEVEL=Debug

# check for proxy and export it.
export X509_USER_PROXY=/etc/proxy/x509up
export X509_CERT_DIR=/etc/grid-security/certificates

while [ ! -f $X509_USER_PROXY ]
do
  sleep 10
  echo "waiting for x509 proxy."
done

ls -lh $X509_USER_PROXY
ls -lh $X509_CERT_DIR

python3 xcache-traces.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem checking rucio traces. Exiting."
    exit $rc
fi