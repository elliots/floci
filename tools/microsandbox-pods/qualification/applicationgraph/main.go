// The same SQL application and AWS SDK worker run on both Kubernetes backends.
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5"
)

type graphWrite struct {
	ID     string `json:"id"`
	Source string `json:"source"`
	Marker string `json:"marker"`
}

type graphApplication struct {
	marker string
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	app := &graphApplication{marker: rand.Text()}
	if os.Getenv("GRAPH_WORKER") == "true" {
		cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"))
		if err != nil {
			slog.Error("AWS SDK configuration failed", "error", err)
			os.Exit(1)
		}
		queue := sqs.NewFromConfig(cfg)
		go app.consume(ctx, queue, os.Getenv("GRAPH_QUEUE_URL"))
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"marker": app.marker})
	})
	mux.HandleFunc("GET /write", app.serve)
	mux.HandleFunc("GET /read", app.serve)
	server := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	context.AfterFunc(ctx, func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	})
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("graph listener failed", "error", err)
		os.Exit(1)
	}
}

func (a *graphApplication) connect(ctx context.Context) (*pgx.Conn, error) {
	return pgx.Connect(ctx, "") // PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE are shared deployment configuration.
}

func (a *graphApplication) insert(ctx context.Context, id, source string) error {
	connection, err := a.connect(ctx)
	if err != nil {
		return err
	}
	defer connection.Close(ctx)
	_, err = connection.Exec(ctx, "INSERT INTO graph_writes(id, source, marker) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING", id, source, a.marker)
	return err
}

func (a *graphApplication) serve(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 150*time.Second)
	defer cancel()
	if r.URL.Path == "/write" {
		id := r.URL.Query().Get("id")
		if id == "" || len(id) > 128 {
			http.Error(w, "missing or oversized id", http.StatusBadRequest)
			return
		}
		if err := a.insert(ctx, id, "http"); err != nil {
			slog.Error("graph HTTP insert failed", "error", err)
			http.Error(w, "database insert failed", http.StatusServiceUnavailable)
			return
		}
	}
	connection, err := a.connect(ctx)
	if err != nil {
		http.Error(w, "database connection failed", http.StatusServiceUnavailable)
		return
	}
	defer connection.Close(ctx)
	rows, err := connection.Query(ctx, "SELECT id, source, marker FROM graph_writes ORDER BY id")
	if err != nil {
		http.Error(w, "database query failed", http.StatusServiceUnavailable)
		return
	}
	values, err := pgx.CollectRows(rows, pgx.RowToStructByName[graphWrite])
	if err != nil {
		http.Error(w, "database result failed", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"marker": a.marker, "writes": values})
}

func (a *graphApplication) consume(ctx context.Context, queue *sqs.Client, url string) {
	for ctx.Err() == nil {
		received, err := queue.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl: new(url), MaxNumberOfMessages: 1, WaitTimeSeconds: 1, VisibilityTimeout: 180,
		})
		if err != nil {
			if ctx.Err() == nil {
				slog.Error("graph SQS receive failed", "error", err)
				select {
				case <-ctx.Done():
				case <-time.After(time.Second):
				}
			}
			continue
		}
		for _, message := range received.Messages {
			operation, cancel := context.WithTimeout(ctx, 150*time.Second)
			err := a.insert(operation, aws.ToString(message.Body), "sqs")
			if err == nil {
				_, err = queue.DeleteMessage(operation, &sqs.DeleteMessageInput{QueueUrl: new(url), ReceiptHandle: message.ReceiptHandle})
			}
			cancel()
			if err != nil {
				slog.Error("graph SQS database write failed", "error", err)
			}
		}
	}
}
