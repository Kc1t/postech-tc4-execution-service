package dynamodb

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/awsx"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/uuid"
)

// Os testes de integração rodam contra o DynamoDB do LocalStack quando AWS_ENDPOINT_URL está
// definida (ex.: http://localhost:4566); sem ela são pulados. Cada teste cria uma tabela própria
// com o mesmo schema de produção, então a ordem e o paralelismo não interferem.
var testClient *ddb.Client

func TestMain(m *testing.M) {
	if os.Getenv("AWS_ENDPOINT_URL") != "" {
		for k, v := range map[string]string{"AWS_REGION": "us-east-1", "AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"} {
			if os.Getenv(k) == "" {
				_ = os.Setenv(k, v)
			}
		}
		cfg, err := awsx.LoadConfig(context.Background())
		if err != nil {
			log.Fatalf("aws config: %v", err)
		}
		testClient = ddb.NewFromConfig(cfg)
	}
	os.Exit(m.Run())
}

// newTable cria uma tabela isolada para o teste e devolve o repositório sobre ela.
func newTable(t *testing.T) *Repository {
	t.Helper()
	if testClient == nil {
		t.Skip("AWS_ENDPOINT_URL não definida: teste de integração com DynamoDB (LocalStack) pulado")
	}
	name := fmt.Sprintf("executions-test-%s", uuid.NewString()[:8])
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := CreateTable(ctx, testClient, name); err != nil {
		t.Fatalf("criar tabela: %v", err)
	}
	t.Cleanup(func() { _ = DeleteTable(context.Background(), testClient, name) })
	return NewRepository(testClient, name)
}

var (
	baseTime = time.Date(2026, 11, 3, 9, 0, 0, 0, time.UTC)
	saga     = entities.ServiceActor("os-service")
	mechanic = entities.Actor{ID: "mec-1", Type: entities.ActorMechanic}
)

func newExecution(t *testing.T, osID string, at time.Time) *entities.Execution {
	t.Helper()
	e, err := entities.NewExecution(entities.NewExecutionParams{
		OSID: osID, OSCode: 7, SagaID: "saga-" + osID,
		Vehicle: entities.Vehicle{Plate: "ABC1D23", Brand: "VW", Model: "Gol", Year: 2019},
		Notes:   "revisão",
	}, saga, at)
	if err != nil {
		t.Fatal(err)
	}
	return e
}
