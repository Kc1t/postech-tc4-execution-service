# Manifestos Kubernetes — execution-service

Os manifestos **não declaram namespace** (o pipeline aplica em `postech` ou `postech-homolog`) e usam os mesmos marcadores do os-service, substituídos no deploy: `IMAGE_PLACEHOLDER`, `NAMESPACE_PLACEHOLDER`, `AWS_REGION_PLACEHOLDER`, `ACCOUNT_ID_PLACEHOLDER`, `CORS_ORIGINS_PLACEHOLDER` e `FAULT_INJECTION_PLACEHOLDER`. A `ResourceQuota`/`LimitRange` do namespace é a do os-service (`oficina-quota`), por isso não está aqui.

| Arquivo | Recurso | Descrição |
|---------|---------|-----------|
| `configmap.yaml` | `ConfigMap` `execution-service-config` | Variáveis não sensíveis: tabela DynamoDB, fila `execution-commands`, tópico `execution-events`, `MAX_CONCURRENT_REPAIRS`, intervalos. |
| `secret.yaml` | `Secret` `execution-service-secret` | `JWT_SECRET` (o mesmo do os-service/Lambda) e `NEW_RELIC_LICENSE_KEY`. Modelo: no CI vem dos GitHub Secrets. |
| `deployment.yaml` | `Deployment` | 2 réplicas, `RollingUpdate` com `maxSurge: 1` / `maxUnavailable: 0`, probes separadas (`startup`/`live` em `/health/live`, `ready` em `/health/ready`), requests/limits, `runAsNonRoot`, `readOnlyRootFilesystem`, `drop: ALL`, `preStop` de 5 s e 30 s de grace period. |
| `service.yaml` | `Service` `execution-service` (ClusterIP) + `execution-service-public` (NLB) | O ClusterIP atende a API composition do os-service; o NLB é o destino do API Gateway para o painel do mecânico. |
| `hpa.yaml` | `HorizontalPodAutoscaler` | 2 a 6 réplicas por CPU (70%) e memória (80%). |
| `pdb.yaml` | `PodDisruptionBudget` | Pelo menos 1 pod durante drenagem de nó. |
| `networkpolicy.yaml` | `NetworkPolicy` | Entrada só na porta 8080. |

## Pré-requisitos na AWS

- Tabela DynamoDB `executions` (PAY_PER_REQUEST, `pk`/`sk` + GSI `gsi1` com `gsi1pk`/`gsi1sk`, projeção ALL) **com TTL no atributo `ttl`** — `scripts/create-table.sh` cria igual ao Terraform.
- Fila `execution-commands` (+DLQ) e tópico `execution-events` assinado por `os-saga-replies` com raw delivery.
- Role do node (LabRole) com `dynamodb:{GetItem,Query,UpdateItem,TransactWriteItems,DescribeTable}`, `sqs:{ReceiveMessage,DeleteMessage,ChangeMessageVisibility}` e `sns:Publish`.

## Aplicar

```bash
NAMESPACE=postech-homolog   # ou postech
kubectl apply -n "$NAMESPACE" -f networkpolicy.yaml -f configmap.yaml -f secret.yaml
kubectl apply -n "$NAMESPACE" -f deployment.yaml -f service.yaml -f hpa.yaml -f pdb.yaml
kubectl rollout status deployment/execution-service -n "$NAMESPACE"
```

Graceful shutdown: no `SIGTERM` o serviço para de aceitar HTTP, espera as requisições em andamento, para o consumidor SQS (mensagens não confirmadas voltam para a fila depois do visibility timeout) e faz um último flush do outbox antes de sair.
