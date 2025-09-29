TAG ?= 0.0.1

build:
	go build -o flutterdb

clean:
	rm -f flutterdb

docker:
	docker build -f deploy/Dockerfile -t github.com/aleph-zero/flutterdb:$(TAG) .

kind-cluster:
	kind create cluster --config deploy/kind-cluster.yaml 
	kind load docker-image github.com/aleph-zero/flutterdb:0.0.1

kind-deploy-helm:
	kubectl create namespace flutterdb
	helm install --namespace flutterdb flutterdb deploy/flutterdb

docker-ecr:
	docker build --platform linux/amd64 -f deploy/Dockerfile -t flutterdb:$(TAG) .
	aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws/p1n2v0n7
	docker tag flutterdb:$(TAG) public.ecr.aws/p1n2v0n7/flutterdb:$(TAG)
	docker push public.ecr.aws/p1n2v0n7/flutterdb:$(TAG)
