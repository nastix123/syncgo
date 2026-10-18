package replication

import (
	"fmt"
	"log/slog"
)

// process decodes a single XLogData payload through the plugin and applies
// the resulting events to the batcher. Transaction boundaries from the plugin
// (EventBegin/EventCommit/EventRollback) drive the commit/rollback semantics
// of the txnTracker so only committed rows reach the batcher.
func (c *LogicalReplicationConn) process(walData []byte) error {
	events, err := c.plugin.Decode(walData)
	if err != nil {
		slog.Error("failed to decode wal data", slog.String("err", err.Error()))
		return fmt.Errorf("failed to decode wal data: %w", err)
	}

	for _, event := range events {
		switch event.Kind {
		case EventBegin:
			c.txn.begin()

		case EventCommit:
			c.txn.commit(c.batcher)

		case EventRollback:
			c.txn.rollback(c.batcher)

		case EventChange:
			slog.Debug("change operation",
				slog.String("action", event.Change.Action.String()),
				slog.String("id", event.Change.ID),
			)
			c.enqueueRow(event.Change)

		default:
			slog.Warn("unknown event kind", slog.Uint64("kind", uint64(event.Kind)))
		}
	}

	return nil
}
