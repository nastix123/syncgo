//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/syncgo/syncgo/e2e/pkg/utils"
	"github.com/syncgo/syncgo/internal/processor"
	"github.com/syncgo/syncgo/pkg/config"
	"github.com/syncgo/syncgo/pkg/metrics"
	"github.com/syncgo/syncgo/pkg/postgresql"
)

const (
	prepareTable     = "sync_prepare_rows"
	prepareIndex     = "sync_prepare_index"
	prepareTimeout   = 30 * time.Second
	prepareSlotName  = "sync_prepare_slot"
	preparePubName   = "sync_prepare_pub"
	prepareSearchURL = "http://localhost:9200"
)

// prepareConfig builds a config pointing prepare at the dedicated table,
// slot, publication and index.
func prepareConfig() *config.Config {
	return &config.Config{
		PostgreSQL: config.PostgreSQLConfig{
			User:            "postgres",
			Password:        "postgres",
			Host:            "localhost",
			Port:            "5432",
			Database:        "postgres",
			PublicationName: preparePubName,
			SlotName:        prepareSlotName,
			Tables:          []string{"public." + prepareTable},
		},
		SearchEngine: config.SearchEngineConfig{
			Name:    "elasticsearch",
			Address: prepareSearchURL,
			Index:   prepareIndex,
		},
	}
}

func TestPrepare_CreatesSlotPublicationAndIndex(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), prepareTimeout)
	defer cancel()

	cfg := prepareConfig()
	pgCfg := postgresql.Config{
		User:     cfg.PostgreSQL.User,
		Password: cfg.PostgreSQL.Password,
		Host:     cfg.PostgreSQL.Host,
		Port:     cfg.PostgreSQL.Port,
		Database: cfg.PostgreSQL.Database,
	}

	// The table referenced by the publication must exist beforehand.
	utils.SetupTable(t, ctx, pgCfg, prepareTable, cfg.PostgreSQL.PublicationName)

	// First run creates the slot, publication and index.
	if err := processor.Prepare(ctx, cfg, metrics.DefaultMetrics); err != nil {
		t.Fatal("e2e; prepare; first run failed; error: ", err)
	}

	// Second run is idempotent: existing objects are left as is, no error.
	if err := processor.Prepare(ctx, cfg, metrics.DefaultMetrics); err != nil {
		t.Fatal("e2e; prepare; second run (idempotency) failed; error: ", err)
	}

	conn, err := postgresql.NewStandard(ctx, pgCfg)
	if err != nil {
		t.Fatal("e2e; prepare; failed to connect to postgres; error: ", err)
	}
	defer conn.Close(ctx)

	slotExists, err := postgresql.ReplicationSlotExists(ctx, conn, cfg.PostgreSQL.SlotName)
	if err != nil {
		t.Fatal("e2e; prepare; failed to check slot; error: ", err)
	}
	if !slotExists {
		t.Fatalf("e2e; prepare; replication slot %q was not created", cfg.PostgreSQL.SlotName)
	}

	pubExists, err := postgresql.PublicationExists(ctx, conn, cfg.PostgreSQL.PublicationName)
	if err != nil {
		t.Fatal("e2e; prepare; failed to check publication; error: ", err)
	}
	if !pubExists {
		t.Fatalf("e2e; prepare; publication %q was not created", cfg.PostgreSQL.PublicationName)
	}

	if ok := indexExists(t, cfg.SearchEngine.Address, prepareIndex); !ok {
		t.Fatalf("e2e; prepare; index %q was not created", prepareIndex)
	}

	// Cleanup.
	utils.MustExec(t, ctx, conn, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", preparePubName))
	_, _ = conn.Exec(ctx, "SELECT pg_drop_replication_slot('"+prepareSlotName+"')").ReadAll()
}

func indexExists(t *testing.T, address, index string) bool {
	t.Helper()

	resp, err := utils.DoRequest(t, "HEAD", fmt.Sprintf("%s/%s", address, index))
	if err != nil {
		t.Fatal("e2e; prepare; failed to check index; error: ", err)
	}
	defer resp.Body.Close()

	return resp.StatusCode == 200
}
