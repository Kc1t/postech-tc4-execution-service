# Manifestos Kubernetes do execution-service

Deploy do execution-service no EKS, namespace `postech` ([ADR-0021](https://github.com/Kc1t/postech-tc4-os-service/blob/main/docs/adr/0021-ambiente-unico-no-eks.md)). O namespace, a `ResourceQuota` e o `LimitRange` (`oficina-quota`) são da plataforma e ficam em [`postech-tc4-infra-k8s/k8s/namespace.yaml`](https://github.com/Kc1t/postech-tc4-infra-k8s/blob/main/k8s/namespace.yaml), porque a cota vale para os três serviços somados.

Quem aplica é o job `deploy` do [CI](../.github/workflows/ci.yml), na `main`. Os manifests não declaram namespace e trazem marcadores que o job substitui: `IMAGE_PLACEHOLDER`, `NAMESPACE_PLACEHOLDER`, `AWS_REGION_PLACEHOLDER`, `ACCOUNT_ID_PLACEHOLDER`, `CORS_ORIGINS_PLACEHOLDER` e `FAULT_INJECTION_PLACEHOLDER`. O job falha se sobrar algum.

## Manifestos

| Arquivo | Recurso | O que define |
|---|---|---|
| `configmap.yaml` | `ConfigMap execution-service-config` | Configuração não sensível: porta, `APP_ENV=prod`, nível de log, nome da app no New Relic (`execution-service-postech`), CORS, tabela `executions`, URL da fila `execution-commands`, ARN do tópico `execution-events`, `MAX_CONCURRENT_REPAIRS`, `FAULT_INJECTION_ENABLED`, intervalo do relay do outbox e `SHUTDOWN_TIMEOUT`. |
| `secret.yaml` | `Secret execution-service-secret` | Só documenta as chaves (`JWT_SECRET`, o mesmo do os-service e da lambda-auth, e `NEW_RELIC_LICENSE_KEY`). **Não é aplicado**: o CI cria o Secret a partir dos GitHub Secrets. |
| `deployment.yaml` | `Deployment execution-service` | 2 réplicas, rolling update com `maxSurge: 1` e `maxUnavailable: 0`, usuário não root (UID 10001), sistema de arquivos só leitura, `drop: ALL`, requests de 100m/96Mi e limits de 500m/256Mi, `preStop` de 5 s e 30 s de grace period. |
| `service.yaml` | `Service execution-service` (ClusterIP) e `execution-service-public` (NLB) | O ClusterIP atende a API composition do os-service (`EXECUTION_SERVICE_URL=http://execution-service`). O NLB é o destino do API Gateway para o painel do mecânico (`/api/v1/executions/*`), que o encontra pela tag `kubernetes.io/service-name=postech/execution-service-public`. |
| `hpa.yaml` | `HorizontalPodAutoscaler` | De 2 a 6 réplicas, com alvo de 70% de CPU e 80% de memória. |
| `pdb.yaml` | `PodDisruptionBudget` | Mantém pelo menos 1 pod durante drain e atualização de nó. |
| `networkpolicy.yaml` | `NetworkPolicy execution-service` | Libera a entrada só na porta 8080/TCP e nega o resto. A saída fica livre, porque o pod fala com DynamoDB, SQS, SNS e New Relic por endpoints públicos. |

Réplicas concorrentes são seguras: o SQS entrega cada comando a um consumidor, o inbox descarta reentregas e o relay do outbox reserva cada item por lease.

## Probes

| Probe | Caminho | Falha significa |
|---|---|---|
| startup | `/health/live` (até 60 s) | O processo ainda está subindo; as outras probes esperam. |
| readiness | `/health/ready` (`DescribeTable` no DynamoDB, 2 s) | O DynamoDB está inacessível: o pod sai do Service, mas não reinicia. |
| liveness | `/health/live` | O processo travou: o kubelet reinicia o pod. |

A liveness não depende da AWS, para uma falha do DynamoDB não reiniciar todos os pods em cascata.

> No EKS, a `NetworkPolicy` só filtra com o addon `vpc-cni` configurado com `enableNetworkPolicy = "true"` ([`addons.tf`](https://github.com/Kc1t/postech-tc4-infra-k8s/blob/main/addons.tf)). Sem isso, o objeto é aceito e não faz nada.

## Pré-requisitos na AWS

- Tabela DynamoDB `executions` (`PAY_PER_REQUEST`, `pk`/`sk` e GSI `gsi1` com `gsi1pk`/`gsi1sk`, projeção ALL) **com TTL no atributo `ttl`**, criada pelo [`infra/`](../infra/) no próprio job de deploy. `scripts/create-table.sh` cria igual, fora do Terraform.
- Fila `execution-commands` (com a DLQ `execution-commands-dlq`) e tópico `execution-events` assinado pela fila `os-saga-replies` com raw delivery, criados pelo [postech-tc4-infra-k8s](https://github.com/Kc1t/postech-tc4-infra-k8s/blob/main/messaging.tf).
- Role do nó (LabRole) com `dynamodb:{GetItem,Query,UpdateItem,TransactWriteItems,DescribeTable}`, `sqs:{ReceiveMessage,DeleteMessage}` e `sns:Publish`, as chamadas que o código faz ([ADR-0016](https://github.com/Kc1t/postech-tc4-os-service/blob/main/docs/adr/0016-credenciais-dos-pods-pela-labrole.md)).

## Aplicar à mão

O fluxo abaixo replica o que o CI faz, com valores de exemplo.

```bash
NAMESPACE=postech
IMAGE=<conta>.dkr.ecr.us-east-1.amazonaws.com/execution-service:<sha>

kubectl create secret generic execution-service-secret -n "$NAMESPACE" \
  --from-literal=JWT_SECRET=... --from-literal=NEW_RELIC_LICENSE_KEY=... \
  --dry-run=client -o yaml | kubectl apply -f -

mkdir -p rendered && cp configmap.yaml deployment.yaml service.yaml hpa.yaml pdb.yaml networkpolicy.yaml rendered/
sed -i "s|IMAGE_PLACEHOLDER|$IMAGE|g; s|NAMESPACE_PLACEHOLDER|$NAMESPACE|g; ..." rendered/*.yaml
kubectl apply -n "$NAMESPACE" -f rendered/
kubectl rollout status deployment/execution-service -n "$NAMESPACE"
kubectl get svc execution-service-public -n "$NAMESPACE"   # EXTERNAL-IP = DNS do NLB
```

## Desligamento gracioso

No `SIGTERM`, depois do `preStop` de 5 s, o serviço para de aceitar HTTP, espera as requisições em andamento, para o consumidor SQS (mensagens não confirmadas voltam para a fila depois do visibility timeout) e faz um último flush do outbox antes de sair. `SHUTDOWN_TIMEOUT` (20 s) mais o `preStop` cabem nos 30 s de `terminationGracePeriodSeconds`.

## Ver a escala

```bash
kubectl get hpa execution-service -n postech -w
kubectl get pods -l app=execution-service -n postech -w
```

O HPA depende do addon `metrics-server`, instalado pelo infra-k8s. Sem ele, os alvos ficam em `<unknown>` e nada escala.
