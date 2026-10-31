# Banco próprio do execution-service: DynamoDB em tabela única (ADR-0012).
#
#   pk = EXEC#{os_id}  sk = STATE          estado atual da execução (com version para o lock otimista)
#   pk = EXEC#{os_id}  sk = EVT#{ts}#{id}  linha do tempo (quem fez o quê e quando)
#   pk = OUTBOX        sk = {ts}#{id}      eventos a publicar (gravados na mesma TransactWriteItems)
#   pk = MSG#{consumer}#{message_id}   sk = MSG   inbox: mensagem já processada (com ttl)
#
# gsi1 é esparso: só o STATE de execuções ativas tem gsi1pk = QUEUE#{status} e
# gsi1sk = {prioridade}#{enfileirada_em}, o que dá a fila do mecânico já ordenada.
locals {
  tags = {
    Project     = var.project_name
    Environment = var.environment
    Service     = "execution-service"
    ManagedBy   = "terraform"
  }
}

# tfsec:ignore:aws-dynamodb-table-customer-key O Learner Lab não permite criar CMK; a tabela usa a chave gerenciada da AWS.
resource "aws_dynamodb_table" "executions" {
  name         = var.table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  attribute {
    name = "gsi1pk"
    type = "S"
  }

  attribute {
    name = "gsi1sk"
    type = "S"
  }

  global_secondary_index {
    name            = "gsi1"
    hash_key        = "gsi1pk"
    range_key       = "gsi1sk"
    projection_type = "ALL"
  }

  # Itens do inbox expiram sozinhos.
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }

  deletion_protection_enabled = false

  tags = local.tags
}
