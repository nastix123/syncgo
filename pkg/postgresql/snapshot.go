package postgresql

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/syncgo/syncgo/internal/bulk_transformer"
	"github.com/syncgo/syncgo/pkg/logger"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// rowConsumer receives rows read under the exported snapshot.
type rowConsumer interface {
	Add(item bulk_transformer.Data)
}

// CopyTablesSnapshot reads the current content of the given tables under the
// exported snapshot and feeds every row into the consumer as an index
// operation. The snapshot must have been exported by the connection that
// created the replication slot; it stays valid only while that connection is
// open, so a separate session binds to it in a REPEATABLE READ read-only
// transaction.
func CopyTablesSnapshot(ctx context.Context, conn *pgconn.PgConn, snapshotName string, tables []string, idColumn string, consumer rowConsumer) error {
	if len(tables) == 0 {
		return nil
	}

	if _, err := conn.Exec(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY").ReadAll(); err != nil {
		return fmt.Errorf("failed to begin snapshot transaction: %w", err)
	}

	setResult := conn.ExecParams(ctx, "SET TRANSACTION SNAPSHOT $1",
		[][]byte{[]byte(snapshotName)}, []uint32{pgtype.TextOID}, nil, nil).Read()
	if setResult.Err != nil {
		_ = rollbackSnapshotTxn(ctx, conn)
		return fmt.Errorf("failed to set transaction snapshot %q: %w", snapshotName, setResult.Err)
	}

	slog.Info("copying tables under exported snapshot",
		slog.String("snapshot", snapshotName),
		slog.String("tables", strings.Join(tables, ", ")),
	)

	for _, table := range tables {
		if err := copyTableSnapshot(ctx, conn, table, idColumn, consumer); err != nil {
			_ = rollbackSnapshotTxn(ctx, conn)
			return err
		}
	}

	if _, err := conn.Exec(ctx, "COMMIT").ReadAll(); err != nil {
		return fmt.Errorf("failed to commit snapshot transaction: %w", err)
	}

	return nil
}

// copyTableSnapshot streams the content of a single table within the open
// snapshot transaction.
func copyTableSnapshot(ctx context.Context, conn *pgconn.PgConn, table string, idColumn string, consumer rowConsumer) error {
	quoted := quoteQualifiedIdent(table)
	rr := conn.Exec(ctx, fmt.Sprintf("SELECT * FROM %s", quoted)).ResultReader()
	fieldDescs := rr.FieldDescriptions()
	typeMap := pgtype.NewMap()

	// Resolve the id column position once per table.
	idIdx := -1
	for i, fd := range fieldDescs {
		if string(fd.Name) == idColumn {
			idIdx = i
			break
		}
	}
	if idIdx == -1 {
		if _, closeErr := rr.Close(); closeErr != nil {
			logger.Error("failed to close table snapshot query", closeErr)
		}
		return fmt.Errorf("id column %q not found in table %q", idColumn, table)
	}

	var count int
	for rr.NextRow() {
		select {
		case <-ctx.Done():
			_, _ = rr.Close()
			return ctx.Err()
		default:
		}

		values := snapshotRowToValues(fieldDescs, rr.Values(), typeMap)
		id, ok := snapshotDocumentID(idColumn, values)
		if !ok {
			slog.Error("missing document id in snapshot row",
				slog.String("table", table),
				slog.String("column", idColumn),
			)
			continue
		}

		body, err := json.Marshal(values)
		if err != nil {
			slog.Error("failed to marshal snapshot row to JSON",
				slog.String("table", table),
				slog.String("err", err.Error()),
			)
			continue
		}

		consumer.Add(bulk_transformer.Data{
			ID:     id,
			Action: bulk_transformer.Index,
			Body:   body,
		})
		count++
	}

	if _, err := rr.Close(); err != nil {
		return fmt.Errorf("failed to read table %q snapshot: %w", table, err)
	}

	slog.Info("copied table snapshot",
		slog.String("table", table),
		slog.Int64("rows", int64(count)),
	)

	return nil
}

// snapshotRowToValues decodes one row using the column OIDs from the row
// description, mirroring the WAL decoding policy: known types are decoded
// via the pgtype map in text format, unknown types fall back to the raw
// string.
func snapshotRowToValues(fieldDescs []pgconn.FieldDescription, rawValues [][]byte, typeMap *pgtype.Map) map[string]any {
	values := make(map[string]any, len(fieldDescs))
	for i, fd := range fieldDescs {
		colName := string(fd.Name)
		if rawValues[i] == nil {
			values[colName] = nil
			continue
		}
		val, err := decodeTextColumnData(typeMap, rawValues[i], fd.DataTypeOID)
		if err != nil {
			slog.Error("error decoding snapshot column",
				slog.String("column", colName),
				slog.String("err", err.Error()),
			)
			continue
		}
		values[colName] = val
	}
	return values
}

// decodeTextColumnData decodes a text-format column value using the pgtype
// map, falling back to the raw string for unknown OIDs.
func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (any, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

func snapshotDocumentID(idColumn string, values map[string]any) (string, bool) {
	if values == nil {
		return "", false
	}
	val, ok := values[idColumn]
	if !ok || val == nil {
		return "", false
	}
	return fmt.Sprint(val), true
}

func rollbackSnapshotTxn(ctx context.Context, conn *pgconn.PgConn) error {
	_, err := conn.Exec(ctx, "ROLLBACK").ReadAll()
	return err
}
