.PHONY: localstack-up localstack-down localstack-build localstack-deploy localstack-smoke localstack-teardown localstack-all

localstack-up:
	docker network create interlock-net 2>/dev/null || true
	docker run -d --name interlock-localstack \
	  --network interlock-net \
	  -p 4566:4566 \
	  -e SERVICES=lambda,dynamodb,dynamodbstreams,events,sqs,stepfunctions,iam,logs \
	  -e LAMBDA_EXECUTOR=docker \
	  -e LAMBDA_DOCKER_NETWORK=interlock-net \
	  -e DEBUG=0 \
	  localstack/localstack:3.7.2
	@echo "Waiting for LocalStack..."
	@until curl -sf http://localhost:4566/_localstack/health > /dev/null; do sleep 2; done
	@echo "LocalStack ready"

localstack-down:
	docker rm -f interlock-localstack || true
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
