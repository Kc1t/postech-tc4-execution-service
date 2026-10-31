#!/usr/bin/env bash
# Cria a tabela única do execution-service (mesmo esquema do Terraform e do init-aws.sh) e liga
# o TTL no atributo `ttl`. Idempotente: se a tabela já existe, só garante o TTL.
#
#   ./scripts/create-table.sh                                  # AWS real (credenciais do ambiente)
#   ENDPOINT_URL=http://localhost:4568 ./scripts/create-table.sh   # LocalStack do docker-compose
set -euo pipefail

TABLE="${DYNAMODB_TABLE:-executions}"
REGION="${AWS_REGION:-us-east-1}"
args=(--region "$REGION")
[ -n "${ENDPOINT_URL:-}" ] && args+=(--endpoint-url "$ENDPOINT_URL")
ddb() { aws dynamodb "${args[@]}" "$@"; }

if ddb describe-table --table-name "$TABLE" >/dev/null 2>&1; then
  echo "tabela ${TABLE} já existe"
else
  # Itens: EXEC#{os_id}/STATE, EXEC#{os_id}/EVT#..., OUTBOX/{ts}#{id} e MSG#.../MSG (inbox).
  # gsi1 é esparso: só o STATE de execuções ativas tem gsi1pk=QUEUE#{status} e
  # gsi1sk={prioridade}#{enfileirada_em}.
  ddb create-table \
    --table-name "$TABLE" \
    --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=S \
      AttributeName=gsi1pk,AttributeType=S AttributeName=gsi1sk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE \
    --global-secondary-indexes '[{"IndexName":"gsi1","KeySchema":[{"AttributeName":"gsi1pk","KeyType":"HASH"},{"AttributeName":"gsi1sk","KeyType":"RANGE"}],"Projection":{"ProjectionType":"ALL"}}]' \
    --billing-mode PAY_PER_REQUEST >/dev/null
  ddb wait table-exists --table-name "$TABLE"
  echo "tabela ${TABLE} criada"
fi

status=$(ddb describe-time-to-live --table-name "$TABLE" --query TimeToLiveDescription.TimeToLiveStatus --output text)
if [ "$status" = "ENABLED" ] || [ "$status" = "ENABLING" ]; then
  echo "ttl já habilitado (${status})"
else
  ddb update-time-to-live --table-name "$TABLE" --time-to-live-specification "Enabled=true,AttributeName=ttl" >/dev/null
  echo "ttl habilitado em ${TABLE}.ttl"
fi
