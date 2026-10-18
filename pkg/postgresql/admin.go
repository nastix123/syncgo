package postgresql

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/syncgo/syncgo/pkg/logger"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// replicationPlugin is the logical decoding output plugin used by syncgo.
const replicationPlugin = "pgoutput"

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
		logger.Error("failed to parse postgres config", err)
		return nil, err
	}

	conn, err := pgconn.ConnectConfig(ctx, pgCfg)
	if err != nil {
		logger.Error("failed to connect to postgres", err)
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
		logger.Error("failed to create publication", err)
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
//
// The slot is created with the pg_create_logical_replication_slot SQL
// function instead of the CREATE_REPLICATION_SLOT replication command,
// because the admin connection is a regular (non-walsender) connection.
// The slot name and plugin are passed as query parameters.
func EnsureReplicationSlot(ctx context.Context, conn *pgconn.PgConn, slotName string) error {
	exists, err := ReplicationSlotExists(ctx, conn, slotName)
	if err != nil {
		return fmt.Errorf("failed to check replication slot %q: %w", slotName, err)
	}
	if exists {
		slog.Info("replication slot already exists", slog.String("name", slotName))
		return nil
	}

	const sql = "SELECT pg_create_logical_replication_slot($1, $2)"
	args := [][]byte{[]byte(slotName), []byte(replicationPlugin)}

	if result := conn.ExecParams(ctx, sql, args, []uint32{pgtype.TextOID, pgtype.TextOID}, nil, nil); !result.NextRow() {
		_, err := result.Close()
		logger.Error("failed to create replication slot", err)
		return fmt.Errorf("failed to create replication slot %q: %w", slotName, err)
	}

	slog.Info("created replication slot", slog.String("name", slotName))

	return nil
}

// ExportedSlot describes a replication slot created with an exported
// snapshot via the streaming replication protocol.
type ExportedSlot struct {
	SlotName      string
	ConsistentLSN pglogrepl.LSN
	SnapshotName  string
}

// CreateReplicationSlotWithSnapshot creates a logical replication slot via
// the streaming replication protocol with snapshot export and returns the
// exported snapshot name along with the slot's consistent point LSN.
//
// The exported snapshot is valid only while conn (the connection that
// created the slot) stays open; the caller must use it before closing the
// connection.
func CreateReplicationSlotWithSnapshot(ctx context.Context, conn *pgconn.PgConn, slotName string) (ExportedSlot, error) {
	result, err := pglogrepl.CreateReplicationSlot(ctx, conn, slotName, replicationPlugin, pglogrepl.CreateReplicationSlotOptions{
		SnapshotAction: "EXPORT_SNAPSHOT",
	})
	if err != nil {
		return ExportedSlot{}, fmt.Errorf("failed to create replication slot %q with snapshot export: %w", slotName, err)
	}

	consistentLSN, err := pglogrepl.ParseLSN(result.ConsistentPoint)
	if err != nil {
		return ExportedSlot{}, fmt.Errorf("failed to parse consistent point %q: %w", result.ConsistentPoint, err)
	}

	return ExportedSlot{
		SlotName:      result.SlotName,
		ConsistentLSN: consistentLSN,
		SnapshotName:  result.SnapshotName,
	}, nil
}

// PublicationTables returns the schema-qualified table list of the given
// publication.
func PublicationTables(ctx context.Context, conn *pgconn.PgConn, pubName string) ([]string, error) {
	const sql = "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = $1"

	result := conn.ExecParams(ctx, sql, [][]byte{[]byte(pubName)}, []uint32{pgtype.TextOID}, nil, nil).Read()
	if result.Err != nil {
		return nil, fmt.Errorf("failed to list tables of publication %q: %w", pubName, result.Err)
	}
	rows := result.Rows

	tables := make([]string, 0, len(rows))
	for _, row := range rows {
		if len(row) != 2 {
			continue
		}
		tables = append(tables, fmt.Sprintf("%s.%s", string(row[0]), string(row[1])))
	}

	return tables, nil
}

// quoteIdent quotes a single SQL identifier (schema, table, publication or
// slot name). Embedded double quotes are doubled per the SQL standard.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// quoteQualifiedIdent quotes a possibly schema-qualified name (e.g.
// "public.users"), quoting each dot-separated part individually. A name
// without a dot is treated as a bare identifier.
func quoteQualifiedIdent(name string) string {
	parts := strings.Split(name, ".")
	quoted := make([]string, len(parts))
	for i, part := range parts {
		quoted[i] = quoteIdent(part)
	}

	return strings.Join(quoted, ".")
}

func quoteIdents(names []string) []string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = quoteQualifiedIdent(name)
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
