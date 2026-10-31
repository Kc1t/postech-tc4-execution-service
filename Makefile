.PHONY: help run build up down logs localstack seed smoke token tidy swagger mocks test test-unit test-integration test-component cover lint

APP_NAME=execution-service
MAIN=./cmd/api
# Pacotes que contam para a cobertura (cmd/, docs/, mocks/, scripts/ e o teste de componente ficam de fora).
COVER_PKGS=$$(go list ./... | grep -v -E '/cmd/|/docs|/mocks|/scripts|/test/')
# LocalStack do docker-compose (porta 4568 no host). Os testes de repositório/relay e o teste de
# componente pulam quando AWS_ENDPOINT_URL não está definida.
LOCALSTACK_URL ?= http://localhost:4568
AWS_TEST_ENV=AWS_ENDPOINT_URL=$(LOCALSTACK_URL) AWS_REGION=us-east-1 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test

help:
	@echo "run | build | up | down | logs | localstack | seed | smoke | token | swagger | mocks | test-unit | test | test-integration | test-component | cover | lint | tidy"

run:
	go run $(MAIN)

build:
	go build -o $(APP_NAME) $(MAIN)

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f execution-service

# Só o LocalStack (para os testes de integração e de componente).
localstack:
	docker compose up -d --wait localstack

# Enfileira OS de exemplo na fila do mecânico (precisa de `make up`).
seed:
	./scripts/seed.sh 3

# Fluxo completo (comandos na fila → API do mecânico → eventos publicados) contra `make up`.
smoke:
	./scripts/smoke.sh

# JWT de mecânico para o Postman (ROLE=admin para administrador).
token:
	@go run ./scripts/gen_token.go -role $${ROLE:-mechanic}

swagger:
	swag init -g $(MAIN)/main.go -o docs --parseInternal

mocks:
	cd internal/ports && for f in usecases repositories; do \
		mockgen -source=./$$f.go -destination=./mocks/$$f.go -package=mocks; done

# Só testes de unidade (os de DynamoDB são pulados sem AWS_ENDPOINT_URL).
test-unit:
	env -u AWS_ENDPOINT_URL go test ./... -count=1

# Unidade + integração com o DynamoDB do LocalStack (precisa de `make localstack`).
test:
	$(AWS_TEST_ENV) go test ./... -count=1

test-integration:
	$(AWS_TEST_ENV) go test ./internal/adapters/outbound/dynamodb/... -count=1 -v

# Sobe o serviço em processo contra SQS/SNS/DynamoDB reais do LocalStack.
test-component:
	$(AWS_TEST_ENV) go test -tags component ./test/component/... -count=1 -v

cover:
	$(AWS_TEST_ENV) go test -count=1 -coverprofile=coverage.out $(COVER_PKGS)
	go tool cover -func=coverage.out | tail -1

lint:
	golangci-lint run ./...

tidy:
	go mod tidy
