//go:build component

// Teste de componente: sobe o execution-service inteiro (o mesmo bootstrap do main: HTTP,
// consumidor SQS e relay do outbox) contra DynamoDB, SQS e SNS reais do LocalStack e verifica
// comando → efeito → evento publicado.
//
//	AWS_ENDPOINT_URL=http://localhost:4566 go test -tags component ./test/component/ -v
//
// Os recursos (tabela, fila de comandos, tópico de eventos e uma fila de teste assinando o
// tópico com raw delivery) são criados com sufixo aleatório e removidos no fim, então o teste
// não disputa mensagens com outra instância do serviço.
package component

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Kc1t/postech-tc4-execution-service/cmd/api/bootstrap"
	"github.com/Kc1t/postech-tc4-execution-service/config"
	dynamorepo "github.com/Kc1t/postech-tc4-execution-service/internal/adapters/outbound/dynamodb"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/awsx"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/contracts"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/messaging"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

const jwtSecret = "segredo-do-teste-de-componente"

type env struct {
	t        *testing.T
	baseURL  string
	token    string
	sqs      *sqs.Client
	pub      *messaging.AWSPublisher
	commands messaging.Destination
	probeURL string

	mu     sync.Mutex
	events []received
}

type received struct {
	env   contracts.Envelope
	attrs map[string]string
}

func setup(t *testing.T) *env {
	t.Helper()
	if os.Getenv("AWS_ENDPOINT_URL") == "" {
		t.Skip("AWS_ENDPOINT_URL não definida: teste de componente precisa do LocalStack")
	}
	for k, v := range map[string]string{"AWS_REGION": "us-east-1", "AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"} {
		if os.Getenv(k) == "" {
			t.Setenv(k, v)
		}
	}
	ctx := context.Background()
	awsCfg, err := awsx.LoadConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ddbClient := dynamodb.NewFromConfig(awsCfg)
	sqsClient := sqs.NewFromConfig(awsCfg)
	snsClient := sns.NewFromConfig(awsCfg)

	suffix := uuid.NewString()[:8]
	table := "executions-ct-" + suffix
	if err := dynamorepo.CreateTable(ctx, ddbClient, table); err != nil {
		t.Fatalf("tabela: %v", err)
	}
	t.Cleanup(func() { _ = dynamorepo.DeleteTable(context.Background(), ddbClient, table) })

	cmdQueue := createQueue(t, sqsClient, "execution-commands-ct-"+suffix)
	probeQueue := createQueue(t, sqsClient, "execution-events-probe-"+suffix)
	topic, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("execution-events-ct-" + suffix)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = snsClient.DeleteTopic(context.Background(), &sns.DeleteTopicInput{TopicArn: topic.TopicArn})
	})
	probeAttrs, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(probeQueue), AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := snsClient.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: topic.TopicArn, Protocol: aws.String("sqs"),
		Endpoint:   aws.String(probeAttrs.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]),
		Attributes: map[string]string{"RawMessageDelivery": "true"},
	}); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{
		AppPort: "0", AppEnv: config.EnvDevelopment, LogLevel: "warn",
		JWTSecret: jwtSecret, CORSAllowedOrigins: []string{"http://localhost:3000"},
		DynamoDBTable: table, SQSExecutionCommandsURL: cmdQueue, SNSExecutionEventsARN: aws.ToString(topic.TopicArn),
		MaxConcurrentRepairs: 10, FaultInjectionEnabled: true,
		OutboxRelayInterval: 100 * time.Millisecond, ShutdownTimeout: 5 * time.Second,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	if testing.Verbose() {
		log = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	}
	app, err := bootstrap.New(ctx, cfg, log, nil)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- app.Run(runCtx, ln) }()
	t.Cleanup(func() {
		stop()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
	})

	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": "mecanico-1", "role": "mechanic", "name": "Mecânico", "exp": time.Now().Add(time.Hour).Unix(),
	}).SignedString([]byte(jwtSecret))
	if err != nil {
		t.Fatal(err)
	}
	return &env{
		t: t, baseURL: "http://" + ln.Addr().String(), token: tok, sqs: sqsClient,
		pub:      messaging.NewAWSPublisher(snsClient, sqsClient),
		commands: messaging.Destination{Kind: messaging.Queue, Name: cmdQueue},
		probeURL: probeQueue,
	}
}

func createQueue(t *testing.T, c *sqs.Client, name string) string {
	t.Helper()
	out, err := c.CreateQueue(context.Background(), &sqs.CreateQueueInput{
		QueueName: aws.String(name), Attributes: map[string]string{"VisibilityTimeout": "5"},
	})
	if err != nil {
		t.Fatalf("fila %s: %v", name, err)
	}
	t.Cleanup(func() { _, _ = c.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: out.QueueUrl}) })
	return aws.ToString(out.QueueUrl)
}

// send publica um comando na fila como o os-service faria (mesmo publicador do chassis).
func (e *env) send(msgType, osID, sagaID string, payload any) contracts.Envelope {
	e.t.Helper()
	envl, err := contracts.NewEnvelope(msgType, "os-service", contracts.Meta{SagaID: sagaID, OSID: osID, CorrelationID: "corr-" + osID}, payload)
	if err != nil {
		e.t.Fatal(err)
	}
	e.resend(envl)
	return envl
}

func (e *env) resend(envl contracts.Envelope) {
	e.t.Helper()
	if err := e.pub.Publish(context.Background(), e.commands, envl); err != nil {
		e.t.Fatalf("publicar %s: %v", envl.Type, err)
	}
}

func (e *env) call(method, path string, body any, headers map[string]string) (int, []byte) {
	e.t.Helper()
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req, _ := http.NewRequest(method, e.baseURL+path, &buf)
	req.Header.Set("Authorization", "Bearer "+e.token)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func (e *env) mustCall(method, path string, body any, headers map[string]string) map[string]any {
	e.t.Helper()
	status, b := e.call(method, path, body, headers)
	if status != http.StatusOK {
		e.t.Fatalf("%s %s: %d %s", method, path, status, b)
	}
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	return out
}

// waitStatus consulta GET /executions/{os_id} até o status esperado (o comando é assíncrono).
func (e *env) waitStatus(osID, want string) map[string]any {
	e.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	last := ""
	for time.Now().Before(deadline) {
		status, b := e.call(http.MethodGet, "/api/v1/executions/"+osID, nil, nil)
		if status == http.StatusOK {
			var out map[string]any
			_ = json.Unmarshal(b, &out)
			if last = fmt.Sprint(out["status"]); last == want {
				return out
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	e.t.Fatalf("execução %s não chegou a %s (último status: %q)", osID, want, last)
	return nil
}

// waitEvent lê a fila de teste (assinante do tópico execution-events) até achar o evento.
func (e *env) waitEvent(osID, msgType string) received {
	e.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if r, ok := e.find(osID, msgType); ok {
			return r
		}
		e.poll(1)
	}
	e.t.Fatalf("evento %s da OS %s não publicado em execution-events", msgType, osID)
	return received{}
}

func (e *env) find(osID, msgType string) (received, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, r := range e.events {
		if r.env.OSID == osID && r.env.Type == msgType {
			return r, true
		}
	}
	return received{}, false
}

func (e *env) count(osID string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for _, r := range e.events {
		if r.env.OSID == osID {
			n++
		}
	}
	return n
}

func (e *env) poll(waitSeconds int32) {
	out, err := e.sqs.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(e.probeURL), MaxNumberOfMessages: 10, WaitTimeSeconds: waitSeconds,
		MessageAttributeNames: []string{"All"},
	})
	if err != nil {
		e.t.Fatalf("ler fila de teste: %v", err)
	}
	for _, m := range out.Messages {
		envl, err := contracts.Parse([]byte(aws.ToString(m.Body)))
		if err != nil {
			e.t.Fatalf("corpo publicado não é envelope (raw delivery): %v", err)
		}
		attrs := map[string]string{}
		for k, v := range m.MessageAttributes {
			attrs[k] = aws.ToString(v.StringValue)
		}
		e.mu.Lock()
		e.events = append(e.events, received{env: envl, attrs: attrs})
		e.mu.Unlock()
		_, _ = e.sqs.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{QueueUrl: aws.String(e.probeURL), ReceiptHandle: m.ReceiptHandle})
	}
}

func startDiagnosis(e *env, osID, sagaID string) {
	e.t.Helper()
	e.send(contracts.CmdStartDiagnosis, osID, sagaID, contracts.StartDiagnosis{
		OSCode: 1001, Vehicle: contracts.Vehicle{Plate: "BRA2E19", Brand: "Fiat", Model: "Argo", Year: 2022}, Notes: "ruído na suspensão",
	})
	e.waitStatus(osID, "queued_diagnosis")
}

func diagnose(e *env, osID string) {
	e.t.Helper()
	e.mustCall(http.MethodPost, "/api/v1/executions/"+osID+"/diagnosis/start", nil, map[string]string{"X-Correlation-ID": "req-start-" + osID})
	e.waitEvent(osID, contracts.EvtDiagnosisStarted)
	e.mustCall(http.MethodPost, "/api/v1/executions/"+osID+"/diagnosis/complete", map[string]any{
		"services": []map[string]any{{"code": 12}},
		"parts":    []map[string]any{{"manufacturer_code": "MONROE-G16", "quantity": 2}},
		"notes":    "amortecedores dianteiros vazando",
	}, map[string]string{"X-Correlation-ID": "req-complete-" + osID})
}

var repairItems = []contracts.Item{
	{Kind: contracts.ItemKindService, Ref: "12", Description: "Troca de amortecedores", Quantity: 1, UnitPrice: 250},
	{Kind: contracts.ItemKindPart, Ref: "MONROE-G16", Description: "Amortecedor dianteiro", Quantity: 2, UnitPrice: 420},
}

func TestComponente_CaminhoFeliz(t *testing.T) {
	e := setup(t)
	osID, sagaID := uuid.NewString(), uuid.NewString()

	startDiagnosis(e, osID, sagaID)
	queue := e.mustCall(http.MethodGet, "/api/v1/executions/queue", nil, nil)
	if queue["count"].(float64) < 1 {
		t.Fatalf("a execução deveria estar na fila: %v", queue)
	}

	diagnose(e, osID)
	ev := e.waitEvent(osID, contracts.EvtDiagnosisCompleted)
	payload, err := contracts.Decode[contracts.DiagnosisCompleted](ev.env)
	if err != nil {
		t.Fatal(err)
	}
	if len(payload.Services) != 1 || payload.Services[0].Code != 12 || len(payload.Parts) != 1 ||
		payload.Parts[0].ManufacturerCode != "MONROE-G16" || payload.Parts[0].Quantity != 2 {
		t.Fatalf("payload do DiagnosisCompleted errado: %+v", payload)
	}
	if ev.env.SagaID != sagaID || ev.env.CorrelationID != "req-complete-"+osID || ev.env.Producer != "execution-service" {
		t.Fatalf("envelope errado: %+v", ev.env)
	}
	if ev.attrs[messaging.AttrMessageType] != contracts.EvtDiagnosisCompleted || ev.attrs[messaging.AttrOSID] != osID || ev.attrs[messaging.AttrSagaID] != sagaID {
		t.Fatalf("atributos de roteamento errados: %v", ev.attrs)
	}
	e.waitStatus(osID, "diagnosed")

	repair := e.send(contracts.CmdStartRepair, osID, sagaID, contracts.StartRepair{OSCode: 1001, Items: repairItems})
	queued := e.waitStatus(osID, "queued_repair")
	if items, _ := queued["items"].([]any); len(items) != 2 {
		t.Fatalf("itens do reparo não gravados: %v", queued["items"])
	}

	e.mustCall(http.MethodPost, "/api/v1/executions/"+osID+"/repair/start", nil, nil)
	e.waitEvent(osID, contracts.EvtRepairStarted)
	e.mustCall(http.MethodPost, "/api/v1/executions/"+osID+"/repair/finish", nil, nil)
	finished := e.waitEvent(osID, contracts.EvtRepairFinished)
	if p, _ := contracts.Decode[contracts.RepairFinished](finished.env); p.FinishedAt.IsZero() {
		t.Fatal("RepairFinished sem finished_at")
	}

	details := e.waitStatus(osID, "finished")
	if tl, _ := details["timeline"].([]any); len(tl) != 6 {
		t.Fatalf("linha do tempo deveria ter 6 registros, tem %d", len(tl))
	}

	// Reentrega do mesmo StartRepair (mesmo message_id): o inbox descarta, sem RepairRejected.
	before := e.count(osID)
	e.resend(repair)
	time.Sleep(1500 * time.Millisecond)
	e.poll(1)
	if _, rejected := e.find(osID, contracts.EvtRepairRejected); rejected || e.count(osID) != before {
		t.Fatal("mensagem duplicada não pode gerar novo evento")
	}
	if again := e.waitStatus(osID, "finished"); again["version"] != details["version"] {
		t.Fatalf("mensagem duplicada não pode alterar o estado: v%v → v%v", details["version"], again["version"])
	}

	// Transição inválida pela API devolve 409 padronizado.
	if status, b := e.call(http.MethodPost, "/api/v1/executions/"+osID+"/repair/start", nil, nil); status != http.StatusConflict ||
		!bytes.Contains(b, []byte(`"INVALID_TRANSITION"`)) {
		t.Fatalf("esperava 409 INVALID_TRANSITION, veio %d %s", status, b)
	}
}

func TestComponente_ReparoRecusadoPorFalhaSimulada(t *testing.T) {
	e := setup(t)
	osID, sagaID := uuid.NewString(), uuid.NewString()

	startDiagnosis(e, osID, sagaID)
	diagnose(e, osID)
	e.waitEvent(osID, contracts.EvtDiagnosisCompleted)

	cmd := e.send(contracts.CmdStartRepair, osID, sagaID, contracts.StartRepair{
		OSCode: 1001, Items: repairItems, SimulateFailure: contracts.FailRepair,
	})
	ev := e.waitEvent(osID, contracts.EvtRepairRejected)
	p, err := contracts.Decode[contracts.RepairRejected](ev.env)
	if err != nil || p.Reason == "" {
		t.Fatalf("RepairRejected sem motivo: %+v %v", p, err)
	}
	if ev.env.CausationID != cmd.MessageID || ev.env.SagaID != sagaID {
		t.Fatalf("o evento aponta para o comando que o causou: %+v", ev.env)
	}
	rejected := e.waitStatus(osID, "rejected")
	if rejected["reason"] != p.Reason {
		t.Fatalf("motivo gravado = %v, publicado = %q", rejected["reason"], p.Reason)
	}

	// Compensação da saga: CancelExecution em status final é ignorado.
	e.send(contracts.CmdCancelExecution, osID, sagaID, contracts.CancelExecution{Reason: "reparo recusado"})
	time.Sleep(time.Second)
	e.poll(1)
	if _, ok := e.find(osID, contracts.EvtExecutionCancelled); ok {
		t.Fatal("execução rejeitada (final) não é cancelada de novo")
	}
}

func TestComponente_Cancelamento(t *testing.T) {
	e := setup(t)
	osID, sagaID := uuid.NewString(), uuid.NewString()

	startDiagnosis(e, osID, sagaID)
	e.send(contracts.CmdCancelExecution, osID, sagaID, contracts.CancelExecution{Reason: "cliente desistiu"})
	ev := e.waitEvent(osID, contracts.EvtExecutionCancelled)
	if p, _ := contracts.Decode[contracts.ExecutionCancelled](ev.env); p.Reason != "cliente desistiu" {
		t.Fatalf("motivo = %q", p.Reason)
	}
	e.waitStatus(osID, "cancelled")

	// Cancelamento que chega antes do StartDiagnosis: a execução nasce cancelada e o
	// StartDiagnosis atrasado não reabre.
	late := uuid.NewString()
	e.send(contracts.CmdCancelExecution, late, sagaID, contracts.CancelExecution{Reason: "saga compensada"})
	e.waitEvent(late, contracts.EvtExecutionCancelled)
	e.send(contracts.CmdStartDiagnosis, late, sagaID, contracts.StartDiagnosis{OSCode: 1})
	time.Sleep(time.Second)
	if got := e.waitStatus(late, "cancelled"); got["version"].(float64) != 1 {
		t.Fatalf("StartDiagnosis atrasado não pode alterar a execução cancelada: %v", got)
	}
}
