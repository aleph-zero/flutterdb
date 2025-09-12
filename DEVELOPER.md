# DEVELOPER

### Working with Kind
https://stackoverflow.com/questions/60487792/kind-cluster-how-to-see-docker-images-that-are-loaded

To see images loaded into `kind` cluster:
```docker exec -it kind-control-plane crictl images```
```docker exec -it kind-worker crictl images```

Setup load balancing:
```sudo cloud-provider-kind```
See instructions: https://github.com/kubernetes-sigs/cloud-provider-kind.

Install via Helm:
```kubectl create namespace flutterdb```
```helm install --namespace flutterdb flutterdb deploy/flutterdb```

To exec into a running container:
```kubectl exec -n flutterdb -it flutterdb-0 -- /bin/sh```

To test the API:
[andryushka flutterdb] [10:03:41] > kubectl get svc -n flutterdb
NAME                  TYPE           CLUSTER-IP      EXTERNAL-IP   PORT(S)          AGE
flutterdb              LoadBalancer   10.96.11.164    172.20.0.6    1234:30821/TCP   72s
flutterdb-membership   ClusterIP      10.96.123.163   <none>        7777/TCP         72s

curl -XGET 'http://172.20.0.6:5678/identity'


### Running with OpenTelemetry
Before running the server set the address of the collector:

export OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4318

### Loading test data

flutterdb client indexer --client.indexer.file ./testdata/documents/cities.ndjson --client.indexer.index cities
flutterdb client indexer --client.indexer.file ./testdata/documents/books.ndjson --client.indexer.index books

