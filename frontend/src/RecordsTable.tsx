import React from 'react';

export type RecordRow = {
  topic: string;
  partition: number;
  offset: number;
  timestamp: number;
  keyBase64: string | null;
  valueBase64: string | null;
  headers: {name: string; base64: string | null}[];
  masked: boolean;
};

const utf8 = new TextDecoder('utf-8', {fatal: true, ignoreBOM: true});

function recordText(base64: string | null, masked: boolean, header?: string): string {
  if (base64 === null) return 'null';
  if (masked) return '[masked]';

  try {
    const bytes = Uint8Array.from(atob(base64), byte => byte.charCodeAt(0));
    const view = new DataView(bytes.buffer);
    if (bytes.length === 4 && (header === 'kafka_dlt-original-partition' || header === 'demo-dlt-hops')) {
      return String(view.getInt32(0));
    }
    if (bytes.length === 8 && header === 'kafka_dlt-original-offset') {
      return String(view.getBigInt64(0));
    }

    const text = utf8.decode(bytes);
    // Even valid UTF-8 can contain control bytes rather than displayable text.
    if (!/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]/.test(text)) {
      return text || '(empty)';
    }
  } catch {
    // Preserve binary values as base64 instead of inserting replacement characters.
  }

  return `base64: ${base64}`;
}

function RecordTimestamp({timestamp}: {timestamp: number}) {
  const date = new Date(timestamp);
  if (timestamp < 0 || Number.isNaN(date.getTime())) return <>Timestamp unavailable</>;

  const iso = date.toISOString();
  return <time dateTime={iso}>{iso.replace('T', ' ').replace('Z', ' UTC')}</time>;
}

export function RecordsTable({records, onReveal, busy}: {
  records: RecordRow[];
  onReveal?: (record: RecordRow) => void;
  busy: boolean;
}) {
  return <div className="table-scroll">
    <table className="records-table">
      <thead><tr><th>Coordinates</th><th>Key</th><th>Payload preview</th><th>Headers</th><th>Actions</th></tr></thead>
      <tbody>{records.map(record => <tr key={`${record.partition}:${record.offset}`}>
        <td>{record.topic}<br/>P{record.partition} · offset {record.offset}<br/><RecordTimestamp timestamp={record.timestamp}/></td>
        <td>{recordText(record.keyBase64, record.masked)}</td>
        <td><pre>{record.valueBase64 === null ? 'null (tombstone)' : recordText(record.valueBase64, record.masked)}</pre></td>
        <td><ol>{record.headers.map((header, index) =>
          <li key={index}><strong>{header.name}</strong> {recordText(header.base64, record.masked, header.name)}</li>
        )}</ol></td>
        <td>{onReveal && record.masked && <button disabled={busy} onClick={() => onReveal(record)}>Reveal record</button>}</td>
      </tr>)}</tbody>
    </table>
  </div>;
}
