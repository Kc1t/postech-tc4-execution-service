#!/usr/bin/env bash
# Popula a fila de trabalho do mecânico publicando comandos execution.StartDiagnosis na fila
# execution-commands, como o os-service faria ao aprovar a abertura de uma OS.
#
#   docker compose up -d && ./scripts/seed.sh 5
set -euo pipefail

COUNT="${1:-3}"
LS="${LOCALSTACK_CONTAINER:-tc4-exec-localstack}"
QUEUE="http://localhost:4566/000000000000/execution-commands"
plates=(BRA2E19 RIO4F21 SPO1A23 BHZ9C45 POA3D67)
models=("Fiat Argo" "VW Gol" "Chevrolet Onix" "Hyundai HB20" "Renault Kwid")

for i in $(seq 1 "$COUNT"); do
  os_id=$(uuidgen | tr '[:upper:]' '[:lower:]')
  idx=$(( (i - 1) % ${#plates[@]} ))
  brand=${models[$idx]%% *}; model=${models[$idx]#* }
  body=$(jq -cn --arg mid "$(uuidgen | tr '[:upper:]' '[:lower:]')" --arg saga "$(uuidgen | tr '[:upper:]' '[:lower:]')" \
    --arg os "$os_id" --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg plate "${plates[$idx]}" \
    --arg brand "$brand" --arg model "$model" --argjson code $((1000 + i)) '{
      message_id: $mid, type: "execution.StartDiagnosis", version: 1, saga_id: $saga, os_id: $os,
      correlation_id: ("seed-" + $os), producer: "os-service", occurred_at: $at,
      payload: {os_code: $code, vehicle: {plate: $plate, brand: $brand, model: $model, year: 2022},
                notes: "OS criada pelo seed"}}')
  docker exec "$LS" awslocal sqs send-message --queue-url "$QUEUE" --message-body "$body" \
    --message-attributes "message_type={DataType=String,StringValue=execution.StartDiagnosis}" >/dev/null
  echo "StartDiagnosis enviado: os_id=${os_id}"
done
