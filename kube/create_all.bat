
kubectl create -f ./namespace.yaml

kubectl delete secret  config -n aaas 
kubectl create secret generic config -n aaas --from-file=secrets/config.json

kubectl create -f  ./secrets/mailgun-secret.yaml

kubectl create -f ./es-cluster-state.yaml
kubectl create -f ./frontier-failed-q.yaml
kubectl create -f ./frontier-threads.yaml
@REM kubectl create -f ./fts-aggregator.yaml
kubectl create -f ./job-task-indexing.yaml
kubectl create -f ./sending-mails.yaml
kubectl create -f ./top-users.yaml
kubectl create -f ./user-reports.yaml
REM cd ..\..\containers\FTS
REM kubectl create -f ./FTS-secret.yaml
REM kubectl create -f ./ES-secret.yaml
REM kubectl create -f ./FTS-deployment.yaml