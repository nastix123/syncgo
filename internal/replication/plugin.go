package replication

import (
	"fmt"
	"sort"
	"strings"

	"github.com/syncgo/syncgo/internal/bulk_transformer"
)

const (
	pgoutputName = "pgoutput"
	wal2jsonName = "wal2json"
)

// EventKind describes the type of a decoded replication event.
type EventKind uint8

const (
	// EventBegin marks the start of a transaction.
	EventBegin EventKind = iota
	// EventCommit marks a successful commit of the current transaction.
	EventCommit
	// EventRollback aborts the current transaction (e.g. a streamed
	// transaction abort).
	EventRollback
	// EventChange carries a single row change in Change.
	EventChange
)

// Event is a single decoded event produced by a Plugin.
type Event struct {
	Kind EventKind
	// Change is set only for EventChange events.
	Change bulk_transformer.Data
}

// Plugin decodes the output of a PostgreSQL logical decoding plugin into
// events. One Plugin instance is bound to a single replication connection,
// so implementations may keep state (e.g. the relation cache of pgoutput).
type Plugin interface {
	// Name returns the output plugin name (e.g. pgoutput).
	Name() string
	// Options returns plugin-specific options passed to START_REPLICATION.
	Options() []string
	// Decode parses a single XLogData payload into a list of events.
	Decode(walData []byte) ([]Event, error)
}

// SupportedPlugins returns the names of all supported logical decoding
// plugins.
func SupportedPlugins() []string {
	return []string{pgoutputName, wal2jsonName}
}

// NewPlugin creates the plugin implementation for the given logical decoding
// plugin name. publicationName and idColumn are only used by plugins that
// need them (pgoutput uses the publication, both use the id column).
func NewPlugin(name, publicationName, idColumn string) (Plugin, error) {
	switch name {
	case "", pgoutputName:
		return newPgOutputPlugin(publicationName, idColumn), nil
	case wal2jsonName:
		return newWal2JSONPlugin(idColumn), nil
	default:
		return nil, fmt.Errorf("unsupported replication plugin %q (supported: %s)",
			name, strings.Join(SupportedPlugins(), ", "))
	}
}

func quotePluginOption(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func sortedPluginNames(names []string) []string {
	sorted := append([]string(nil), names...)
	sort.Strings(sorted)
	return sorted
}
