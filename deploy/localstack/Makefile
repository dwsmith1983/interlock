.PHONY: localstack-up localstack-down localstack-build localstack-deploy localstack-smoke localstack-teardown localstack-all

localstack-up:
	docker network create interlock-net 2>/dev/null || true
	docker run -d --name interlock-localstack \
	  --network interlock-net --network-alias localstack \
	  -p 4566:4566 \
	  -v /var/run/docker.sock:/var/run/docker.sock \
	  -e SERVICES=lambda,dynamodb,dynamodbstreams,events,sqs,stepfunctions,iam,logs \
	  -e LAMBDA_DOCKER_NETWORK=interlock-net \
	  -e DEBUG=0 \
	  localstack/localstack:3.7.2
	@echo "Waiting for LocalStack..."
	@until curl -sf http://localhost:4566/_localstack/health > /dev/null; do sleep 2; done
	@echo "LocalStack ready"

# Removing LocalStack leaves the Lambda containers it started on interlock-net.
localstack-down:
	docker rm -f interlock-localstack || true
	@ids=$$(docker ps -aq --filter network=interlock-net); \
	if [ -n "$$ids" ]; then echo "removing Lambda containers on interlock-net"; docker rm -f $$ids >/dev/null; fi
	docker network rm interlock-net 2>/dev/null || true

localstack-build:
	bash deploy/localstack/build.sh

localstack-deploy: localstack-build
	python deploy/localstack/deploy.py deploy

localstack-teardown:
	python deploy/localstack/deploy.py teardown

localstack-smoke:
	python deploy/localstack/deploy.py smoke

localstack-all: localstack-up localstack-deploy localstack-smoke
