#!/usr/bin/env bash
# Teste de fumaça do execution-service isolado: o os-service é simulado publicando os comandos
# na fila execution-commands do LocalStack, o mecânico é simulado pela API e os eventos são
# lidos da fila os-saga-replies (assinante do tópico execution-events).
#
#   docker compose up -d --build --wait && ./scripts/smoke.sh
set -euo pipefail
cd "$(dirname "$0")/.."

API="${API:-http://localhost:8083/api/v1}"
LS="${LOCALSTACK_CONTAINER:-tc4-exec-localstack}"
COMMANDS="http://localhost:4566/000000000000/execution-commands"
REPLIES="http://localhost:4566/000000000000/os-saga-replies"
TOKEN=$(go run ./scripts/gen_token.go -role mechanic -sub mec-smoke)
aws() { docker exec "$LS" awslocal "$@"; }
step() { printf '\n\033[1;34m▶ %s\033[0m\n' "$*"; }
uuid() { uuidgen | tr '[:upper:]' '[:lower:]'; }
api() { curl -sS -X "$1" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -H "X-Correlation-ID: smoke-$OS" ${3:+-d "$3"} "$API$2"; }

send() { # tipo payload
  local body
  body=$(jq -cn --arg mid "$(uuid)" --arg type "$1" --arg saga "$SAGA" --arg os "$OS" \
    --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --argjson payload "$2" \
    '{message_id:$mid, type:$type, version:1, saga_id:$saga, os_id:$os, producer:"os-service", occurred_at:$at, payload:$payload}')
  aws sqs send-message --queue-url "$COMMANDS" --message-body "$body" \
    --message-attributes "message_type={DataType=String,StringValue=$1}" >/dev/null
}

wait_status() {
  for _ in $(seq 1 40); do
    [ "$(api GET "/executions/$OS" | jq -r .status)" = "$1" ] && { echo "status: $1"; return; }
    sleep 0.25
  done
  echo "execução não chegou a $1" >&2; exit 1
}

# Lê os eventos desta OS em os-saga-replies (consumindo-os) até ver o tipo esperado.
wait_event() {
  for _ in $(seq 1 20); do
    local batch
    batch=$(aws sqs receive-message --queue-url "$REPLIES" --max-number-of-messages 10 --wait-time-seconds 1 \
      --query 'Messages[].[ReceiptHandle,Body]' --output json)
    [ "$batch" = "null" ] && continue
    echo "$batch" | jq -r '.[][0]' | while read -r rh; do aws sqs delete-message --queue-url "$REPLIES" --receipt-handle "$rh"; done
    if echo "$batch" | jq -e --arg os "$OS" --arg t "$1" 'any(.[][1] | fromjson; .os_id == $os and .type == $t)' >/dev/null; then
      echo "evento publicado: $1"; return
    fi
  done
  echo "evento $1 não publicado" >&2; exit 1
}

OS=$(uuid); SAGA=$(uuid)
step "StartDiagnosis (os_id=$OS)"
send execution.StartDiagnosis '{"os_code":4242,"vehicle":{"plate":"BRA2E19","brand":"Fiat","model":"Argo","year":2022}}'
wait_status queued_diagnosis
api GET "/executions/queue?status=queued_diagnosis" | jq -c '{count, primeiro: .items[0].os_id}'

step "Mecânico inicia e conclui o diagnóstico"
api POST "/executions/$OS/diagnosis/start" >/dev/null && wait_event execution.DiagnosisStarted
api POST "/executions/$OS/diagnosis/complete" \
  '{"services":[{"code":12}],"parts":[{"manufacturer_code":"MONROE-G16","quantity":2}],"notes":"amortecedores vazando"}' >/dev/null
wait_event execution.DiagnosisCompleted

step "StartRepair (orçamento aprovado e pago)"
send execution.StartRepair '{"os_code":4242,"items":[{"kind":"service","ref":"12","description":"Troca de amortecedores","quantity":1,"unit_price":250}]}'
wait_status queued_repair

step "Mecânico executa o reparo"
api POST "/executions/$OS/repair/start" >/dev/null && wait_event execution.RepairStarted
api POST "/executions/$OS/repair/finish" >/dev/null && wait_event execution.RepairFinished
api GET "/executions/$OS" | jq -c '{status, version, timeline: [.timeline[].to_status]}'

step "Transição inválida → 409"
api POST "/executions/$OS/repair/start" | jq -c .

printf '\n\033[1;32m✔ fluxo do execution-service ok\033[0m\n'
