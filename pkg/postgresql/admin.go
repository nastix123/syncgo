package postgresql

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

func NewStandard(ctx context.Context, cfg Config) (*pgconn.PgConn, error) {
	url := fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.Database,
	)

	pgCfg, err := pgconn.ParseConfig(url)
	if err != nil {
		slog.Error("failed to parse postgres config", slog.String("error", err.Error()))
		return nil, err
	}

	conn, err := pgconn.ConnectConfig(ctx, pgCfg)
	if err != nil {
		slog.Error("failed to connect to postgres", slog.String("error", err.Error()))
		return nil, err
	}

	return conn, nil
}

func ReplicationSlotExists(ctx context.Context, conn *pgconn.PgConn, slotName string) (bool, error) {
	return queryExists(ctx, conn,
		"SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
		slotName,
	)
}

func PublicationExists(ctx context.Context, conn *pgconn.PgConn, pubName string) (bool, error) {
	return queryExists(ctx, conn,
		"SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
		pubName,
	)
}

// EnsurePublication creates the publication for the given tables if it does
// not exist yet. It is idempotent: an existing publication is left as is.
func EnsurePublication(ctx context.Context, conn *pgconn.PgConn, pubName string, tables []string) error {
	exists, err := PublicationExists(ctx, conn, pubName)
	if err != nil {
		return fmt.Errorf("failed to check publication %q: %w", pubName, err)
	}
	if exists {
		slog.Info("publication already exists", slog.String("name", pubName))
		return nil
	}

	sql := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s",
		quoteIdent(pubName), strings.Join(quoteIdents(tables), ", "))

	if _, err := conn.Exec(ctx, sql).ReadAll(); err != nil {
		slog.Error("failed to create publication", slog.String("err", err.Error()))
		return err
	}

	slog.Info("created publication",
		slog.String("name", pubName),
		slog.String("tables", strings.Join(tables, ", ")),
	)

	return nil
}

// EnsureReplicationSlot creates the logical replication slot if it does not
// exist yet. It is idempotent: an existing slot is left as is.
func EnsureReplicationSlot(ctx context.Context, conn *pgconn.PgConn, slotName string) error {
	exists, err := ReplicationSlotExists(ctx, conn, slotName)
	if err != nil {
		return fmt.Errorf("failed to check replication slot %q: %w", slotName, err)
	}
	if exists {
		slog.Info("replication slot already exists", slog.String("name", slotName))
		return nil
	}

	sql := fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL pgoutput", quoteIdent(slotName))

	if _, err := conn.Exec(ctx, sql).ReadAll(); err != nil {
		slog.Error("failed to create replication slot", slog.String("err", err.Error()))
		return err
	}

	slog.Info("created replication slot", slog.String("name", slotName))

	return nil
}

func quoteIdent(name string) string {
	return fmt.Sprintf("%q", name)
}

func quoteIdents(names []string) []string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = quoteIdent(name)
	}

	return quoted
}

func queryExists(ctx context.Context, conn *pgconn.PgConn, sql string, arg string) (bool, error) {
	result := conn.ExecParams(ctx, sql, [][]byte{[]byte(arg)}, []uint32{pgtype.TextOID}, nil, nil)
	if !result.NextRow() {
		_, err := result.Close()
		return false, fmt.Errorf("exists query returned no rows: %w", err)
	}

	values := result.Values()
	if len(values) != 1 {
		_, err := result.Close()
		return false, fmt.Errorf("expected one column in exists query result: %w", err)
	}

	exists := string(values[0]) == "t"
	_, err := result.Close()
	return exists, err
}
