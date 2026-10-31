package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// TableAdmin é o subconjunto do cliente usado para criar e remover a tabela.
type TableAdmin interface {
	CreateTable(ctx context.Context, in *ddb.CreateTableInput, optFns ...func(*ddb.Options)) (*ddb.CreateTableOutput, error)
	DeleteTable(ctx context.Context, in *ddb.DeleteTableInput, optFns ...func(*ddb.Options)) (*ddb.DeleteTableOutput, error)
	DescribeTable(ctx context.Context, in *ddb.DescribeTableInput, optFns ...func(*ddb.Options)) (*ddb.DescribeTableOutput, error)
	UpdateTimeToLive(ctx context.Context, in *ddb.UpdateTimeToLiveInput, optFns ...func(*ddb.Options)) (*ddb.UpdateTimeToLiveOutput, error)
}

// TableSchema descreve a tabela única: chaves pk/sk, gsi1 (gsi1pk/gsi1sk) com projeção ALL e
// cobrança sob demanda. É a mesma definição do init-aws.sh, de scripts/create-table.sh e do
// Terraform; aqui serve aos testes, que criam uma tabela isolada por execução.
func TableSchema(name string) *ddb.CreateTableInput {
	s := types.ScalarAttributeTypeS
	return &ddb.CreateTableInput{
		TableName: aws.String(name),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(attrPK), AttributeType: s},
			{AttributeName: aws.String(attrSK), AttributeType: s},
			{AttributeName: aws.String(attrGSI1PK), AttributeType: s},
			{AttributeName: aws.String(attrGSI1SK), AttributeType: s},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String(attrSK), KeyType: types.KeyTypeRange},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{{
			IndexName: aws.String(IndexGSI1),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(attrGSI1PK), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String(attrGSI1SK), KeyType: types.KeyTypeRange},
			},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
		}},
		BillingMode: types.BillingModePayPerRequest,
	}
}

// CreateTable cria a tabela, espera ficar ativa e liga o TTL no atributo "ttl".
func CreateTable(ctx context.Context, api TableAdmin, name string) error {
	if _, err := api.CreateTable(ctx, TableSchema(name)); err != nil {
		var inUse *types.ResourceInUseException
		if !errors.As(err, &inUse) {
			return fmt.Errorf("dynamodb: criar tabela %s: %w", name, err)
		}
	}
	waiter := ddb.NewTableExistsWaiter(api)
	if err := waiter.Wait(ctx, &ddb.DescribeTableInput{TableName: aws.String(name)}, 30*time.Second); err != nil {
		return fmt.Errorf("dynamodb: aguardar tabela %s: %w", name, err)
	}
	_, err := api.UpdateTimeToLive(ctx, &ddb.UpdateTimeToLiveInput{
		TableName:               aws.String(name),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{AttributeName: aws.String("ttl"), Enabled: aws.Bool(true)},
	})
	if err != nil {
		return fmt.Errorf("dynamodb: ligar TTL em %s: %w", name, err)
	}
	return nil
}

// DeleteTable remove a tabela (testes).
func DeleteTable(ctx context.Context, api TableAdmin, name string) error {
	_, err := api.DeleteTable(ctx, &ddb.DeleteTableInput{TableName: aws.String(name)})
	return err
}
