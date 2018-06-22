package clickhouse

import (
	"github.com/mdspinc/clickhouse/lib/data"
	"github.com/mdspinc/clickhouse/lib/protocol"
)

func (ch *clickhouse) sendQuery(query string) error {
	ch.logf("[send query] %s", query)
	if err := ch.encoder.Uvarint(protocol.ClientQuery); err != nil {
		return err
	}
	if err := ch.encoder.String(""); err != nil {
		return err
	}
	{ // client info
		ch.encoder.Uvarint(1)
		ch.encoder.String("")
		ch.encoder.String("") //initial_query_id
		ch.encoder.String("[::ffff:127.0.0.1]:0")
		ch.encoder.Uvarint(1) // iface type TCP
		ch.encoder.String(hostname)
		ch.encoder.String(hostname)
	}
	if err := ch.ClientInfo.Write(ch.encoder); err != nil {
		return err
	}
	if ch.ServerInfo.Revision >= protocol.DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
		ch.encoder.String("")
	}

	if err := ch.encoder.String(""); err != nil { // settings
		return err
	}
	if err := ch.encoder.Uvarint(protocol.StateComplete); err != nil {
		return err
	}
	compress := protocol.CompressDisable
	if ch.compress {
		compress = protocol.CompressEnable
	}
	if err := ch.encoder.Uvarint(compress); err != nil {
		return err
	}
	if err := ch.encoder.String(query); err != nil {
		return err
	}
	if err := ch.writeBlock(&data.Block{}); err != nil {
		return err
	}
	return ch.buffer.Flush()
}
