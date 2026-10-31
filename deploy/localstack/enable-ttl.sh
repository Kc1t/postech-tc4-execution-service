#!/bin/bash
# Liga o TTL (atributo `ttl`) na tabela do execution-service. Roda depois do init-aws.sh
# (montado como init-zz-*.sh, a ordem do ready.d é alfabética). O TTL expira os registros de
# inbox (15 dias) e os itens de outbox já publicados (24 h). No EKS quem liga é o Terraform.
set -euo pipefail

TABLE="${DYNAMODB_TABLE:-executions}"
awslocal dynamodb wait table-exists --table-name "$TABLE"
awslocal dynamodb update-time-to-live --table-name "$TABLE" \
  --time-to-live-specification "Enabled=true,AttributeName=ttl" >/dev/null
echo "ttl habilitado: ${TABLE}.ttl"
