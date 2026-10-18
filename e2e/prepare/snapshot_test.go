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
	snapshotTable   = "sync_snapshot_rows"
	snapshotIndex   = "sync_snapshot_index"
	snapshotTimeout = 60 * time.Second
)

// snapshotConfig builds a config with the export_snapshot option enabled.
func snapshotConfig() *config.Config {
	cfg := prepareConfig()
	cfg.PostgreSQL.PublicationName = "sync_snapshot_pub"
	cfg.PostgreSQL.SlotName = "sync_snapshot_slot"
	cfg.PostgreSQL.Tables = []string{"public." + snapshotTable}
	cfg.PostgreSQL.ExportSnapshot = true
	cfg.SearchEngine.Index = snapshotIndex

	return cfg
}

// TestPrepare_ExportSnapshot verifies that the prepare command with
// export_snapshot enabled:
//  1. copies the current table content into the index (initial load), and
//  2. keeps the change stream consistent with the snapshot, so rows
//     inserted after prepare (but before run) are also synced.
func TestPrepare_ExportSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeout)
	defer cancel()

	cfg := snapshotConfig()
	pgCfg := utils.PgConfigFrom(cfg)

	utils.SetupTable(t, ctx, pgCfg, snapshotTable)

	// Rows that exist before prepare must be copied by the snapshot load.
	utils.InsertRow(t, ctx, pgCfg, snapshotTable, 1, "snapshot-alice")
	utils.InsertRow(t, ctx, pgCfg, snapshotTable, 2, "snapshot-bob")

	if err := processor.Prepare(ctx, cfg, metrics.DefaultMetrics); err != nil {
		t.Fatal("e2e; snapshot; prepare failed; error: ", err)
	}

	// Rows loaded by the snapshot must be present in the index.
	for _, row := range []struct {
		id   int
		name string
	}{
		{1, "snapshot-alice"},
		{2, "snapshot-bob"},
	} {
		utils.AssertDocument(t, cfg.SearchEngine.Address, snapshotIndex, fmt.Sprint(row.id), row.name)
	}

	// Second prepare run is idempotent: the slot exists, the snapshot load
	// is skipped, no error.
	if err := processor.Prepare(ctx, cfg, metrics.DefaultMetrics); err != nil {
		t.Fatal("e2e; snapshot; second prepare failed; error: ", err)
	}

	// A row committed after the snapshot (but before run) must still reach
	// the index through the change stream: run resumes from the slot
	// position, not from the current WAL head.
	utils.InsertRow(t, ctx, pgCfg, snapshotTable, 3, "post-prepare")

	stop := utils.RunProcessor(t, ctx, cfg)
	defer stop()

	utils.AssertDocument(t, cfg.SearchEngine.Address, snapshotIndex, "3", "post-prepare")

	// Cleanup.
	conn, err := postgresql.NewStandard(ctx, pgCfg)
	if err != nil {
		t.Fatal("e2e; snapshot; failed to connect to postgres; error: ", err)
	}
	defer conn.Close(ctx)

	utils.MustExec(t, ctx, conn, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", cfg.PostgreSQL.PublicationName))
	_, _ = conn.Exec(ctx, "SELECT pg_drop_replication_slot('"+cfg.PostgreSQL.SlotName+"')").ReadAll()
}
