kubectl create -f ./namespace.yaml
secrets/google-secret.bat
kubectl create -f  ./secrets/mailgun-secret.yaml
kubectl create -f ./es-cluster-state.yaml
kubectl create -f ./frontier-failed-q.yaml
kubectl create -f ./frontier-threads.yaml
kubectl create -f ./fts-aggregator.yaml
kubectl create -f ./job-task-indexing.yaml
kubectl create -f ./packet-loss.yaml
kubectl create -f ./ps-indexing.yaml
kubectl create -f ./sending-mails.yaml
kubectl create -f ./top-users.yaml
kubectl create -f ./user-reports.yaml
cd ..\..\containers\
kubectl create -f ./FTS-secret.yaml
kubectl create -f ./FTS-deployment.yaml