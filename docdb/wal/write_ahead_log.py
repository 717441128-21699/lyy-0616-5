import os
import struct
import json
import threading
import time
from typing import List, Optional, Dict, Any, Tuple, Iterator
from dataclasses import dataclass, field
from enum import Enum

from ..common import Config, Document, DocumentID


class WALOperation(Enum):
    BEGIN = 1
    COMMIT = 2
    ABORT = 3
    INSERT = 4
    UPDATE = 5
    DELETE = 6
    CHECKPOINT = 7
    INDEX_INSERT = 8
    INDEX_DELETE = 9
    INDEX_UPDATE = 10


@dataclass
class WALRecord:
    lsn: int
    txn_id: int
    operation: WALOperation
    timestamp: float
    collection: Optional[str] = None
    doc_id: Optional[DocumentID] = None
    old_document: Optional[Document] = None
    new_document: Optional[Document] = None
    index_name: Optional[str] = None
    index_key: Optional[Any] = None
    index_value: Optional[Any] = None
    old_index_value: Optional[Any] = None
    new_index_value: Optional[Any] = None
    checkpoint_lsn: Optional[int] = None

    def to_bytes(self) -> bytes:
        data = {
            'lsn': self.lsn,
            'txn_id': self.txn_id,
            'op': self.operation.value,
            'ts': self.timestamp,
        }

        if self.collection:
            data['coll'] = self.collection
        if self.doc_id:
            data['doc_id'] = self.doc_id
        if self.old_document:
            data['old_doc'] = json.loads(self.old_document.to_json())
        if self.new_document:
            data['new_doc'] = json.loads(self.new_document.to_json())
        if self.index_name:
            data['idx_name'] = self.index_name
        if self.index_key is not None:
            data['idx_key'] = self.index_key
        if self.index_value is not None:
            data['idx_val'] = self.index_value
        if self.old_index_value is not None:
            data['old_idx_val'] = self.old_index_value
        if self.new_index_value is not None:
            data['new_idx_val'] = self.new_index_value
        if self.checkpoint_lsn is not None:
            data['ckpt_lsn'] = self.checkpoint_lsn

        json_str = json.dumps(data, ensure_ascii=False)
        json_bytes = json_str.encode('utf-8')
        length = len(json_bytes)
        crc = self._crc32(json_bytes)

        return struct.pack('<II', length, crc) + json_bytes

    @staticmethod
    def from_bytes(data: bytes) -> 'WALRecord':
        json_str = data.decode('utf-8')
        obj = json.loads(json_str)

        old_doc = None
        if 'old_doc' in obj:
            old_doc = Document.from_json(json.dumps(obj['old_doc']))

        new_doc = None
        if 'new_doc' in obj:
            new_doc = Document.from_json(json.dumps(obj['new_doc']))

        return WALRecord(
            lsn=obj['lsn'],
            txn_id=obj['txn_id'],
            operation=WALOperation(obj['op']),
            timestamp=obj['ts'],
            collection=obj.get('coll'),
            doc_id=obj.get('doc_id'),
            old_document=old_doc,
            new_document=new_doc,
            index_name=obj.get('idx_name'),
            index_key=obj.get('idx_key'),
            index_value=obj.get('idx_val'),
            old_index_value=obj.get('old_idx_val'),
            new_index_value=obj.get('new_idx_val'),
            checkpoint_lsn=obj.get('ckpt_lsn'),
        )

    @staticmethod
    def _crc32(data: bytes) -> int:
        crc = 0xFFFFFFFF
        for byte in data:
            crc ^= byte
            for _ in range(8):
                if crc & 1:
                    crc = (crc >> 1) ^ 0xEDB88320
                else:
                    crc >>= 1
        return crc ^ 0xFFFFFFFF


class WriteAheadLog:
    def __init__(self, config: Config):
        self.config = config
        self._lock = threading.RLock()
        self._current_lsn = 0
        self._checkpoint_lsn = 0
        self._wal_file = os.path.join(config.wal_dir, "wal.log")
        self._checkpoint_file = os.path.join(config.wal_dir, "checkpoint.json")
        self._file_handle: Optional[Any] = None

        self._initialize()

    def _initialize(self) -> None:
        os.makedirs(self.config.wal_dir, exist_ok=True)

        if os.path.exists(self._checkpoint_file):
            with open(self._checkpoint_file, 'r', encoding='utf-8') as f:
                ckpt = json.load(f)
                self._checkpoint_lsn = ckpt.get('checkpoint_lsn', 0)
                self._current_lsn = ckpt.get('last_lsn', 0)

        if os.path.exists(self._wal_file):
            last_record = self._get_last_record()
            if last_record:
                self._current_lsn = max(self._current_lsn, last_record.lsn)

        self._file_handle = open(self._wal_file, 'ab')

    def _get_last_record(self) -> Optional[WALRecord]:
        if not os.path.exists(self._wal_file):
            return None

        file_size = os.path.getsize(self._wal_file)
        if file_size == 0:
            return None

        with open(self._wal_file, 'rb') as f:
            pos = file_size

            while pos > 8:
                f.seek(pos - 8)
                header = f.read(8)
                if len(header) < 8:
                    break

                length, crc = struct.unpack('<II', header)

                if pos - 8 - length >= 0:
                    f.seek(pos - 8 - length)
                    record_data = f.read(length)

                    if WALRecord._crc32(record_data) == crc:
                        try:
                            return WALRecord.from_bytes(record_data)
                        except Exception:
                            pass

                pos -= 1

            return None

    def _allocate_lsn(self) -> int:
        self._current_lsn += 1
        return self._current_lsn

    def _write_record(self, record: WALRecord) -> int:
        with self._lock:
            record.lsn = self._allocate_lsn()
            record_bytes = record.to_bytes()

            self._file_handle.write(record_bytes)
            self._file_handle.flush()
            os.fsync(self._file_handle.fileno())

            return record.lsn

    def begin_transaction(self, txn_id: int) -> int:
        record = WALRecord(
            lsn=0,
            txn_id=txn_id,
            operation=WALOperation.BEGIN,
            timestamp=time.time(),
        )
        return self._write_record(record)

    def commit_transaction(self, txn_id: int) -> int:
        record = WALRecord(
            lsn=0,
            txn_id=txn_id,
            operation=WALOperation.COMMIT,
            timestamp=time.time(),
        )
        return self._write_record(record)

    def abort_transaction(self, txn_id: int) -> int:
        record = WALRecord(
            lsn=0,
            txn_id=txn_id,
            operation=WALOperation.ABORT,
            timestamp=time.time(),
        )
        return self._write_record(record)

    def log_insert(self, txn_id: int, collection: str, doc: Document) -> int:
        record = WALRecord(
            lsn=0,
            txn_id=txn_id,
            operation=WALOperation.INSERT,
            timestamp=time.time(),
            collection=collection,
            doc_id=doc.doc_id,
            new_document=doc,
        )
        return self._write_record(record)

    def log_update(self, txn_id: int, collection: str,
                   old_doc: Document, new_doc: Document) -> int:
        record = WALRecord(
            lsn=0,
            txn_id=txn_id,
            operation=WALOperation.UPDATE,
            timestamp=time.time(),
            collection=collection,
            doc_id=old_doc.doc_id,
            old_document=old_doc,
            new_document=new_doc,
        )
        return self._write_record(record)

    def log_delete(self, txn_id: int, collection: str, doc: Document) -> int:
        record = WALRecord(
            lsn=0,
            txn_id=txn_id,
            operation=WALOperation.DELETE,
            timestamp=time.time(),
            collection=collection,
            doc_id=doc.doc_id,
            old_document=doc,
        )
        return self._write_record(record)

    def log_index_insert(self, txn_id: int, index_name: str,
                         key: Any, value: Any) -> int:
        record = WALRecord(
            lsn=0,
            txn_id=txn_id,
            operation=WALOperation.INDEX_INSERT,
            timestamp=time.time(),
            index_name=index_name,
            index_key=key,
            index_value=value,
        )
        return self._write_record(record)

    def log_index_delete(self, txn_id: int, index_name: str,
                         key: Any, value: Any) -> int:
        record = WALRecord(
            lsn=0,
            txn_id=txn_id,
            operation=WALOperation.INDEX_DELETE,
            timestamp=time.time(),
            index_name=index_name,
            index_key=key,
            index_value=value,
        )
        return self._write_record(record)

    def log_index_update(self, txn_id: int, index_name: str, key: Any,
                         old_value: Any, new_value: Any) -> int:
        record = WALRecord(
            lsn=0,
            txn_id=txn_id,
            operation=WALOperation.INDEX_UPDATE,
            timestamp=time.time(),
            index_name=index_name,
            index_key=key,
            old_index_value=old_value,
            new_index_value=new_value,
        )
        return self._write_record(record)

    def log_checkpoint(self) -> int:
        with self._lock:
            self._checkpoint_lsn = self._current_lsn
            record = WALRecord(
                lsn=0,
                txn_id=-1,
                operation=WALOperation.CHECKPOINT,
                timestamp=time.time(),
                checkpoint_lsn=self._checkpoint_lsn,
            )
            lsn = self._write_record(record)

            with open(self._checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'checkpoint_lsn': self._checkpoint_lsn,
                    'last_lsn': self._current_lsn,
                    'timestamp': time.time(),
                }, f, ensure_ascii=False, indent=2)

            return lsn

    def iterate_from(self, start_lsn: int = 0) -> Iterator[WALRecord]:
        if not os.path.exists(self._wal_file):
            return

        with open(self._wal_file, 'rb') as f:
            while True:
                header = f.read(8)
                if len(header) < 8:
                    break

                length, crc = struct.unpack('<II', header)
                record_data = f.read(length)

                if len(record_data) < length:
                    break

                if WALRecord._crc32(record_data) != crc:
                    break

                try:
                    record = WALRecord.from_bytes(record_data)
                    if record.lsn >= start_lsn:
                        yield record
                except Exception:
                    continue

    def get_checkpoint_lsn(self) -> int:
        return self._checkpoint_lsn

    def get_current_lsn(self) -> int:
        return self._current_lsn

    def get_active_transactions(self, after_lsn: int = 0) -> Dict[int, List[WALRecord]]:
        transactions: Dict[int, List[WALRecord]] = {}
        completed: set = set()

        for record in self.iterate_from(after_lsn):
            txn_id = record.txn_id
            if txn_id < 0:
                continue

            if record.operation == WALOperation.BEGIN:
                if txn_id not in transactions:
                    transactions[txn_id] = []
            elif record.operation in (WALOperation.COMMIT, WALOperation.ABORT):
                completed.add(txn_id)
                if txn_id in transactions:
                    del transactions[txn_id]
            elif txn_id in transactions:
                transactions[txn_id].append(record)

        return transactions

    def truncate_before(self, lsn: int) -> None:
        with self._lock:
            temp_file = self._wal_file + ".tmp"

            with open(temp_file, 'wb') as out_f:
                for record in self.iterate_from(lsn):
                    out_f.write(record.to_bytes())

            self._file_handle.close()
            os.replace(temp_file, self._wal_file)
            self._file_handle = open(self._wal_file, 'ab')

    def close(self) -> None:
        with self._lock:
            if self._file_handle:
                self._file_handle.flush()
                os.fsync(self._file_handle.fileno())
                self._file_handle.close()
                self._file_handle = None

    def __del__(self):
        self.close()
