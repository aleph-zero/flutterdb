TAG ?= 0.0.1

build:
	go build -o flutterdb

clean:
	rm -f flutterdb

docker:
	docker build -f deploy/Dockerfile -t github.com/aleph-zero/flutterdb:$(TAG) .

cluster:
	kind create cluster --config deploy/kind-cluster.yaml 
	kind load docker-image github.com/aleph-zero/flutterdb:0.0.1

deploy-helm:
	kubectl create namespace flutterdb
	helm install --namespace flutterdb flutterdb deploy/flutterdb
