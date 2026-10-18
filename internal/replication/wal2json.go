package replication

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/syncgo/syncgo/internal/bulk_transformer"
)

// wal2jsonPlugin decodes the wal2json stream (format-version 2), where each
// XLogData payload is a JSON object: {"action":"B"/"I"/"U"/"D"/"T"/"C", ...}.
// Unlike pgoutput, relation metadata is embedded in the change events, so no
// relation cache is needed.
//
// See https://github.com/eulerto/wal2json for the format description.
type wal2jsonPlugin struct {
	idColumn string
}

func newWal2JSONPlugin(idColumn string) *wal2jsonPlugin {
	return &wal2jsonPlugin{idColumn: idColumn}
}

func (p *wal2jsonPlugin) Name() string {
	return wal2jsonName
}

func (p *wal2jsonPlugin) Options() []string {
	return []string{
		`"format-version" '2'`,
		`"include-timestamp" 'false'`,
		`"include-types" 'false'`,
		`"include-domains" 'false'`,
		`"include-typmod" 'false'`,
		`"include-domain-data-type" 'false'`,
		`"include-column-positions" 'false'`,
		`"include-not-null" 'false'`,
		`"include-default" 'false'`,
		`"include-lsn" 'false'`,
		`"include-xids" 'false'`,
		`"include-unchanged-toast" 'false'`,
		`"write-in-chunks" 'false'`,
	}
}

// wal2jsonChange is a single change record from a format-version 2 message.
// Values are raw JSON values; unchanged TOAST values arrive as the string
// "unchanged-toast-delta" placeholder or are omitted entirely.
type wal2jsonChange struct {
	Kind    string                     `json:"action"`
	Schema  string                     `json:"schema"`
	Table   string                     `json:"table"`
	Columns []wal2jsonColumn           `json:"columns"`
	OldKeys map[string]json.RawMessage `json:"oldkeys"`
}

type wal2jsonColumn struct {
	Name  string          `json:"name"`
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

// Decode parses a single wal2json format-version 2 XLogData payload. With
// format-version 2 every payload is exactly one record — either a
// transaction boundary ({"action":"B"|"C"|"X"}) or a row change with
// top-level schema/table/columns fields (the array form of format-version 1
// is not used here).
func (p *wal2jsonPlugin) Decode(walData []byte) ([]Event, error) {
	var ch wal2jsonChange
	if err := json.Unmarshal(walData, &ch); err != nil {
		return nil, fmt.Errorf("failed to parse wal2json message: %w", err)
	}

	switch ch.Kind {
	case "B":
		return []Event{{Kind: EventBegin}}, nil
	case "C":
		return []Event{{Kind: EventCommit}}, nil
	case "X":
		return []Event{{Kind: EventRollback}}, nil
	case "I", "U", "D":
		item, err := p.changeToData(ch)
		if err != nil {
			return nil, err
		}
		if item == nil {
			return nil, nil
		}
		return []Event{{Kind: EventChange, Change: *item}}, nil
	case "T":
		slog.Debug("wal2json truncate operation")
		return nil, nil
	case "M":
		slog.Info("wal2json logical decoding message")
		return nil, nil
	default:
		slog.Warn("unknown wal2json action", slog.String("action", ch.Kind))
		return nil, nil
	}
}

// changeToData converts a single wal2json change into a bulk_transformer.Data
// item. It returns (nil, nil) when the change should be skipped (e.g. the id
// column is missing).
func (p *wal2jsonPlugin) changeToData(ch wal2jsonChange) (*bulk_transformer.Data, error) {
	switch ch.Kind {
	case "I":
		values := columnsToValues(ch.Columns)
		id, ok := documentID(p.idColumn, values)
		if !ok {
			p.logMissingID(ch, "insert")
			return nil, nil
		}
		return rowData(id, bulk_transformer.Index, values)

	case "U":
		oldValues := oldKeysToValues(ch.OldKeys)
		id, ok := documentID(p.idColumn, oldValues)
		if !ok {
			values := columnsToValues(ch.Columns)
			id, ok = documentID(p.idColumn, values)
			if !ok {
				p.logMissingID(ch, "update")
				return nil, nil
			}
		}
		values := columnsToValues(ch.Columns)
		if values == nil {
			// REPLICA IDENTITY FULL not set: new values can be absent for
			// unchanged TOAST columns; emit an update with old keys as body.
			values = oldValues
		}
		return rowData(id, bulk_transformer.Update, values)

	case "D":
		values := oldKeysToValues(ch.OldKeys)
		id, ok := documentID(p.idColumn, values)
		if !ok {
			p.logMissingID(ch, "delete")
			return nil, nil
		}
		data := deleteData(id)
		return &data, nil

	default:
		return nil, fmt.Errorf("unexpected wal2json change action %q", ch.Kind)
	}
}

func columnsToValues(cols []wal2jsonColumn) map[string]any {
	if len(cols) == 0 {
		return nil
	}
	values := make(map[string]any, len(cols))
	for _, col := range cols {
		if len(col.Value) == 0 {
			// unchanged TOAST placeholder — omit from the document body
			continue
		}
		var val any
		if err := json.Unmarshal(col.Value, &val); err != nil {
			slog.Error("failed to decode wal2json column",
				slog.String("column", col.Name),
				slog.String("err", err.Error()),
			)
			continue
		}
		values[col.Name] = val
	}
	return values
}

func oldKeysToValues(oldKeys map[string]json.RawMessage) map[string]any {
	if len(oldKeys) == 0 {
		return nil
	}
	values := make(map[string]any, len(oldKeys))
	for name, raw := range oldKeys {
		var val any
		if err := json.Unmarshal(raw, &val); err != nil {
			slog.Error("failed to decode wal2json oldkeys column",
				slog.String("column", name),
				slog.String("err", err.Error()),
			)
			continue
		}
		values[name] = val
	}
	return values
}

func rowData(id string, action bulk_transformer.Action, values map[string]any) (*bulk_transformer.Data, error) {
	body, err := json.Marshal(values)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal row to JSON: %w", err)
	}
	data := bulk_transformer.Data{
		ID:     id,
		Action: action,
		Body:   body,
	}
	return &data, nil
}

func (p *wal2jsonPlugin) logMissingID(ch wal2jsonChange, operation string) {
	slog.Error("missing document id column",
		slog.String("operation", operation),
		slog.String("column", p.idColumn),
		slog.String("relation", ch.Table),
		slog.String("namespace", ch.Schema),
	)
}
