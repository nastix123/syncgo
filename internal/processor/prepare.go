package processor

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/syncgo/syncgo/internal/batcher"
	"github.com/syncgo/syncgo/internal/bulk_transformer"
	"github.com/syncgo/syncgo/pkg/config"
	"github.com/syncgo/syncgo/pkg/logger"
	"github.com/syncgo/syncgo/pkg/metrics"
	"github.com/syncgo/syncgo/pkg/postgresql"
	"github.com/syncgo/syncgo/pkg/search_engine_client"

	"github.com/jackc/pgx/v5/pgconn"
)

const prepareTimeout = 30 * time.Second

// Prepare creates the PostgreSQL publication and replication slot and the
// search engine index. Every step is idempotent: existing objects are left
// as is, so the command can be run multiple times safely.
//
// When postgresql.export_snapshot is enabled and the slot does not exist
// yet, the slot is created with an exported snapshot and the current content
// of the publication tables is copied into the index (initial load). The
// snapshot is taken at the slot's consistent point, so the load is
// transactionally consistent with the change stream: no rows are lost or
// duplicated when the run command continues from the slot position.
func Prepare(parentCtx context.Context, cfg *config.Config, monitoring *metrics.Metrics) error {
	ctx, cancel := context.WithTimeout(parentCtx, prepareTimeout)
	defer cancel()

	if err := preparePublication(ctx, cfg); err != nil {
		return err
	}

	// The index must exist before the snapshot load starts feeding rows in.
	if err := prepareIndex(ctx, cfg, monitoring); err != nil {
		return err
	}

	if err := prepareSlot(ctx, cfg, monitoring); err != nil {
		return err
	}

	return nil
}

// preparePublication ensures the configured publication when a table list is
// provided.
func preparePublication(ctx context.Context, cfg *config.Config) error {
	if len(cfg.PostgreSQL.Tables) == 0 {
		slog.Info("no tables configured, skipping publication management",
			slog.String("publication", cfg.PostgreSQL.PublicationName),
		)
		return nil
	}

	adminConn, err := postgresql.NewStandard(ctx, cfg.PostgreSQL.Config())
	if err != nil {
		return fmt.Errorf("failed to connect to postgres: %w", err)
	}
	defer func() {
		if closeErr := adminConn.Close(ctx); closeErr != nil {
			logger.Error("failed to close admin connection", closeErr)
		}
	}()

	if err := postgresql.EnsurePublication(ctx, adminConn, cfg.PostgreSQL.PublicationName, cfg.PostgreSQL.Tables); err != nil {
		return fmt.Errorf("failed to ensure publication %q: %w", cfg.PostgreSQL.PublicationName, err)
	}

	return nil
}

// prepareSlot ensures the replication slot exists. When snapshot export is
// enabled, the slot is created through the streaming replication protocol
// and the exported snapshot is used for the initial table load.
func prepareSlot(ctx context.Context, cfg *config.Config, monitoring *metrics.Metrics) error {
	adminConn, err := postgresql.NewStandard(ctx, cfg.PostgreSQL.Config())
	if err != nil {
		return fmt.Errorf("failed to connect to postgres: %w", err)
	}
	defer func() {
		if closeErr := adminConn.Close(ctx); closeErr != nil {
			logger.Error("failed to close admin connection", closeErr)
		}
	}()

	exists, err := postgresql.ReplicationSlotExists(ctx, adminConn, cfg.PostgreSQL.SlotName)
	if err != nil {
		return fmt.Errorf("failed to check replication slot %q: %w", cfg.PostgreSQL.SlotName, err)
	}

	if exists {
		slog.Info("replication slot already exists", slog.String("name", cfg.PostgreSQL.SlotName))
		if cfg.PostgreSQL.ExportSnapshot {
			slog.Warn("slot already exists, skipping snapshot load; " +
				"delete the slot (or recreate the index and slot) to re-run the initial load")
		}
		return nil
	}

	if !cfg.PostgreSQL.ExportSnapshot {
		if err := postgresql.EnsureReplicationSlot(ctx, adminConn, cfg.PostgreSQL.SlotName); err != nil {
			return fmt.Errorf("failed to ensure replication slot %q: %w", cfg.PostgreSQL.SlotName, err)
		}
		return nil
	}

	return createSlotAndLoadSnapshot(ctx, cfg, monitoring)
}

// createSlotAndLoadSnapshot creates the replication slot via the streaming
// replication protocol with snapshot export and copies the publication
// tables into the search engine index under the exported snapshot.
//
// The exported snapshot is valid only while the replication connection that
// created the slot stays open, so the connection is closed only after the
// copy completes.
func createSlotAndLoadSnapshot(ctx context.Context, cfg *config.Config, monitoring *metrics.Metrics) error {
	replicationConn, err := postgresql.New(ctx, cfg.PostgreSQL.Config())
	if err != nil {
		return fmt.Errorf("failed to connect to postgres (replication): %w", err)
	}
	defer func() {
		if closeErr := replicationConn.Close(ctx); closeErr != nil {
			logger.Error("failed to close replication connection", closeErr)
		}
	}()

	exported, err := postgresql.CreateReplicationSlotWithSnapshot(ctx, replicationConn, cfg.PostgreSQL.SlotName)
	if err != nil {
		return err
	}

	slog.Info("created replication slot with exported snapshot",
		slog.String("slot", exported.SlotName),
		slog.String("snapshot", exported.SnapshotName),
		slog.String("consistentLSN", exported.ConsistentLSN.String()),
	)

	// The snapshot can only be read from a separate session while the
	// creating connection is still open.
	snapshotConn, err := postgresql.NewStandard(ctx, cfg.PostgreSQL.Config())
	if err != nil {
		return fmt.Errorf("failed to connect to postgres (snapshot): %w", err)
	}
	defer func() {
		if closeErr := snapshotConn.Close(ctx); closeErr != nil {
			logger.Error("failed to close snapshot connection", closeErr)
		}
	}()

	tables, err := snapshotTables(ctx, snapshotConn, cfg)
	if err != nil {
		return err
	}

	b, err := initBatcherForLoad(ctx, cfg, monitoring)
	if err != nil {
		return err
	}
	defer b.Flush()

	if err := postgresql.CopyTablesSnapshot(ctx, snapshotConn, exported.SnapshotName, tables, cfg.PostgreSQL.IDColumn, batcherCommitter{b}); err != nil {
		return fmt.Errorf("failed to copy tables snapshot: %w", err)
	}

	return nil
}

// initBatcherForLoad creates a search engine client and a batcher for the
// snapshot load. The caller must Flush and Close via the returned cleanup.
func initBatcherForLoad(ctx context.Context, cfg *config.Config, monitoring *metrics.Metrics) (*batcher.Batcher, error) {
	client, err := initClient(ctx, cfg.SearchEngine, monitoring)
	if err != nil {
		return nil, fmt.Errorf("failed to create search engine client: %w", err)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			logger.Error("failed to close search engine client", closeErr)
		}
	}()

	return batcher.NewBatcher(ctx, cfg.Batcher.Size, cfg.Batcher.FlushInterval, client, monitoring), nil
}

// snapshotTables returns the tables to copy: the configured list, or — when
// none is configured — the tables of the publication.
func snapshotTables(ctx context.Context, conn *pgconn.PgConn, cfg *config.Config) ([]string, error) {
	if len(cfg.PostgreSQL.Tables) > 0 {
		return cfg.PostgreSQL.Tables, nil
	}

	tables, err := postgresql.PublicationTables(ctx, conn, cfg.PostgreSQL.PublicationName)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables of publication %q: %w", cfg.PostgreSQL.PublicationName, err)
	}
	if len(tables) == 0 {
		return nil, fmt.Errorf("publication %q has no tables to snapshot", cfg.PostgreSQL.PublicationName)
	}

	return tables, nil
}

// batcherCommitter adapts the batcher to the snapshot row consumer: every
// snapshot row is committed immediately so the size-triggered flush sends
// only durable rows.
type batcherCommitter struct {
	b *batcher.Batcher
}

func (c batcherCommitter) Add(item bulk_transformer.Data) {
	c.b.Add(item)
	c.b.Commit()
}

// prepareIndex ensures the search engine index exists.
func prepareIndex(ctx context.Context, cfg *config.Config, monitoring *metrics.Metrics) error {
	client, err := search_engine_client.New(ctx, search_engine_client.Config{
		Name:              cfg.SearchEngine.Name,
		Address:           cfg.SearchEngine.Address,
		Username:          cfg.SearchEngine.Username,
		Password:          cfg.SearchEngine.Password,
		Index:             cfg.SearchEngine.Index,
		ConnectionTimeout: cfg.SearchEngine.ConnectionTimeout,
		GzipCompression:   cfg.SearchEngine.GzipCompression,
		TLS:               searchTLSToClient(cfg.SearchEngine.TLS),
		KeepAlive:         searchKeepAliveToClient(cfg.SearchEngine.KeepAlive),
	}, monitoring)
	if err != nil {
		return fmt.Errorf("failed to create search engine client: %w", err)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			logger.Error("failed to close search engine client", closeErr)
		}
	}()

	exists, err := client.IndexExists(ctx, cfg.SearchEngine.Index)
	if err != nil {
		return fmt.Errorf("failed to check index %q: %w", cfg.SearchEngine.Index, err)
	}

	if exists {
		slog.Info("index already exists", slog.String("index", cfg.SearchEngine.Index))
		return nil
	}

	if err := client.CreateIndex(ctx, cfg.SearchEngine.Index, nil); err != nil {
		return fmt.Errorf("failed to create index %q: %w", cfg.SearchEngine.Index, err)
	}

	slog.Info("created index", slog.String("index", cfg.SearchEngine.Index))

	return nil
}
