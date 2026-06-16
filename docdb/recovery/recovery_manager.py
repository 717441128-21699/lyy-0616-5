import os
import json
import threading
import time
from typing import Dict, List, Optional, Any, Tuple, Set
from dataclasses import dataclass, field
from collections import defaultdict

from ..common import Config, Document, DocumentID, DocumentNotFoundError
from ..storage import DocumentStore
from ..index import IndexManager
from ..wal import WriteAheadLog, WALOperation, WALRecord
from ..concurrency import LockManager
from ..transaction import TransactionManager


@dataclass
class RecoveryStats:
    checkpoint_lsn: int = 0
    recovered_from_lsn: int = 0
    recovered_to_lsn: int = 0
    redo_count: int = 0
    undo_count: int = 0
    committed_txns_recovered: int = 0
    aborted_txns_recovered: int = 0
    inconsistent_indexes: int = 0
    fixed_indexes: int = 0
    orphaned_documents: int = 0
    dangling_index_entries: int = 0
    recovery_time_ms: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'checkpoint_lsn': self.checkpoint_lsn,
            'recovered_from_lsn': self.recovered_from_lsn,
            'recovered_to_lsn': self.recovered_to_lsn,
            'redo_count': self.redo_count,
            'undo_count': self.undo_count,
            'committed_txns_recovered': self.committed_txns_recovered,
            'aborted_txns_recovered': self.aborted_txns_recovered,
            'inconsistent_indexes': self.inconsistent_indexes,
            'fixed_indexes': self.fixed_indexes,
            'orphaned_documents': self.orphaned_documents,
            'dangling_index_entries': self.dangling_index_entries,
            'recovery_time_ms': self.recovery_time_ms,
        }


class RecoveryManager:
    def __init__(self, config: Config, wal: WriteAheadLog,
                 index_manager: IndexManager, lock_manager: LockManager,
                 transaction_manager: TransactionManager):
        self.config = config
        self.wal = wal
        self.index_manager = index_manager
        self.lock_manager = lock_manager
        self.transaction_manager = transaction_manager

        self._lock = threading.RLock()
        self._document_stores: Dict[str, DocumentStore] = {}
        self._recovery_stats_file = os.path.join(config.data_dir, "_recovery_stats.json")

    def register_collection(self, collection: str, store: DocumentStore) -> None:
        with self._lock:
            self._document_stores[collection] = store

    def needs_recovery(self) -> bool:
        checkpoint_lsn = self.wal.get_checkpoint_lsn()
        current_lsn = self.wal.get_current_lsn()

        if current_lsn > checkpoint_lsn:
            return True

        active_txns = self.wal.get_active_transactions(checkpoint_lsn)
        if active_txns:
            return True

        return False

    def recover(self) -> RecoveryStats:
        start_time = time.time()
        stats = RecoveryStats()

        with self._lock:
            checkpoint_lsn = self.wal.get_checkpoint_lsn()
            stats.checkpoint_lsn = checkpoint_lsn
            stats.recovered_from_lsn = checkpoint_lsn

            analysis_result = self._analyze_log(checkpoint_lsn)
            committed_txns = analysis_result['committed']
            in_flight_txns = analysis_result['in_flight']

            self._redo_phase(checkpoint_lsn, committed_txns, stats)
            self._undo_phase(in_flight_txns, stats)

            consistency_report = self._check_consistency()
            stats.inconsistent_indexes = consistency_report['inconsistent_indexes']
            stats.dangling_index_entries = consistency_report['dangling_entries']
            stats.orphaned_documents = consistency_report['orphaned_docs']

            stats.fixed_indexes = self._fix_inconsistencies(consistency_report)

            self.wal.log_checkpoint()

            stats.recovered_to_lsn = self.wal.get_current_lsn()
            stats.recovery_time_ms = (time.time() - start_time) * 1000

            self._save_recovery_stats(stats)

        return stats

    def _analyze_log(self, start_lsn: int) -> Dict[str, Any]:
        committed: Set[int] = set()
        in_flight: Dict[int, List[WALRecord]] = {}
        txn_starts: Set[int] = set()

        for record in self.wal.iterate_from(start_lsn):
            txn_id = record.txn_id

            if txn_id < 0:
                continue

            if record.operation == WALOperation.BEGIN:
                txn_starts.add(txn_id)
                if txn_id not in in_flight:
                    in_flight[txn_id] = []

            elif record.operation == WALOperation.COMMIT:
                committed.add(txn_id)
                if txn_id in in_flight:
                    del in_flight[txn_id]
                if txn_id in txn_starts:
                    txn_starts.discard(txn_id)

            elif record.operation == WALOperation.ABORT:
                if txn_id in in_flight:
                    del in_flight[txn_id]
                if txn_id in txn_starts:
                    txn_starts.discard(txn_id)

            elif txn_id in in_flight:
                in_flight[txn_id].append(record)

            elif txn_id not in committed and txn_id not in in_flight and txn_id in txn_starts:
                in_flight[txn_id] = [record]

        return {
            'committed': committed,
            'in_flight': in_flight,
        }

    def _redo_phase(self, start_lsn: int, committed_txns: Set[int],
                    stats: RecoveryStats) -> None:
        operations = defaultdict(list)

        for record in self.wal.iterate_from(start_lsn):
            if record.operation in (WALOperation.INSERT, WALOperation.UPDATE, WALOperation.DELETE):
                if record.txn_id in committed_txns:
                    operations[record.txn_id].append(record)
            elif record.operation in (WALOperation.INDEX_INSERT, WALOperation.INDEX_DELETE, WALOperation.INDEX_UPDATE):
                if record.txn_id in committed_txns:
                    operations[record.txn_id].append(record)

        for txn_id, txn_operations in operations.items():
            for record in txn_operations:
                try:
                    self._redo_operation(record)
                    stats.redo_count += 1
                except Exception as e:
                    pass

            stats.committed_txns_recovered += 1

    def _redo_operation(self, record: WALRecord) -> None:
        if record.operation == WALOperation.INSERT:
            if record.collection and record.new_document:
                store = self._document_stores.get(record.collection)
                if store and not store.exists(record.new_document.doc_id):
                    store.insert(record.new_document)
                    self.index_manager.on_document_inserted(
                        record.collection, record.new_document
                    )

        elif record.operation == WALOperation.UPDATE:
            if record.collection and record.new_document and record.old_document:
                store = self._document_stores.get(record.collection)
                if store and store.exists(record.new_document.doc_id):
                    old_values = {}
                    existing_doc = store.get(record.new_document.doc_id)
                    for idx in self.index_manager.list_indexes(record.collection):
                        old_values[idx.field] = existing_doc.get_field(idx.field)

                    store.update(record.new_document.doc_id, record.new_document.data)
                    self.index_manager.on_document_updated(
                        record.collection, record.new_document, old_values
                    )

        elif record.operation == WALOperation.DELETE:
            if record.collection and record.old_document:
                store = self._document_stores.get(record.collection)
                if store and store.exists(record.old_document.doc_id):
                    store.delete(record.old_document.doc_id)
                    self.index_manager.on_document_deleted(
                        record.collection, record.old_document
                    )

        elif record.operation == WALOperation.INDEX_INSERT:
            if record.index_name and record.index_key is not None:
                parts = record.index_name.split('.', 1)
                if len(parts) == 2:
                    collection, field = parts
                    idx = self.index_manager.get_index(collection, field)
                    if idx and idx.btree:
                        existing = idx.btree.search(record.index_key)
                        if existing is None:
                            if idx.unique:
                                idx.btree.insert(record.index_key, record.index_value)
                            else:
                                idx.btree.insert(record.index_key, [record.index_value])
                        elif isinstance(existing, list):
                            if record.index_value not in existing:
                                existing.append(record.index_value)
                                idx.btree._save_to_disk()

        elif record.operation == WALOperation.INDEX_DELETE:
            if record.index_name and record.index_key is not None:
                parts = record.index_name.split('.', 1)
                if len(parts) == 2:
                    collection, field = parts
                    idx = self.index_manager.get_index(collection, field)
                    if idx and idx.btree:
                        idx.btree.delete(record.index_key, record.index_value)

    def _undo_phase(self, in_flight_txns: Dict[int, List[WALRecord]],
                    stats: RecoveryStats) -> None:
        for txn_id, operations in in_flight_txns.items():
            for record in reversed(operations):
                try:
                    self._undo_operation(record)
                    stats.undo_count += 1
                except Exception as e:
                    pass

            stats.aborted_txns_recovered += 1
            self.wal.abort_transaction(txn_id)

    def _undo_operation(self, record: WALRecord) -> None:
        if record.operation == WALOperation.INSERT:
            if record.collection and record.new_document:
                store = self._document_stores.get(record.collection)
                if store and store.exists(record.new_document.doc_id):
                    try:
                        doc = store.get(record.new_document.doc_id)
                        store.delete(record.new_document.doc_id)
                        self.index_manager.on_document_deleted(record.collection, doc)
                    except DocumentNotFoundError:
                        pass

        elif record.operation == WALOperation.UPDATE:
            if record.collection and record.old_document:
                store = self._document_stores.get(record.collection)
                if store and store.exists(record.old_document.doc_id):
                    old_values = {}
                    existing_doc = store.get(record.old_document.doc_id)
                    for idx in self.index_manager.list_indexes(record.collection):
                        old_values[idx.field] = existing_doc.get_field(idx.field)

                    store.update(record.old_document.doc_id, record.old_document.data)
                    self.index_manager.on_document_updated(
                        record.collection, record.old_document, old_values
                    )

        elif record.operation == WALOperation.DELETE:
            if record.collection and record.old_document:
                store = self._document_stores.get(record.collection)
                if store and not store.exists(record.old_document.doc_id):
                    store.insert(record.old_document)
                    self.index_manager.on_document_inserted(
                        record.collection, record.old_document
                    )

        elif record.operation == WALOperation.INDEX_INSERT:
            if record.index_name and record.index_key is not None:
                parts = record.index_name.split('.', 1)
                if len(parts) == 2:
                    collection, field = parts
                    idx = self.index_manager.get_index(collection, field)
                    if idx and idx.btree:
                        idx.btree.delete(record.index_key, record.index_value)

        elif record.operation == WALOperation.INDEX_DELETE:
            if record.index_name and record.index_key is not None:
                parts = record.index_name.split('.', 1)
                if len(parts) == 2:
                    collection, field = parts
                    idx = self.index_manager.get_index(collection, field)
                    if idx and idx.btree:
                        existing = idx.btree.search(record.index_key)
                        if existing is None:
                            if idx.unique:
                                idx.btree.insert(record.index_key, record.index_value)
                            else:
                                idx.btree.insert(record.index_key, [record.index_value])
                        elif isinstance(existing, list):
                            if record.index_value not in existing:
                                existing.append(record.index_value)
                                idx.btree._save_to_disk()

    def _check_consistency(self) -> Dict[str, Any]:
        report = {
            'inconsistent_indexes': 0,
            'dangling_entries': 0,
            'orphaned_docs': 0,
            'details': [],
        }

        for collection, store in self._document_stores.items():
            all_docs = store.get_all()
            doc_ids = {doc.doc_id for doc in all_docs}

            indexes = self.index_manager.list_indexes(collection)

            for idx in indexes:
                indexed_doc_ids: Set[DocumentID] = set()
                dangling_count = 0

                all_entries = idx.btree.get_all()
                for key, value in all_entries:
                    if isinstance(value, list):
                        for doc_id in value:
                            indexed_doc_ids.add(doc_id)
                            if doc_id not in doc_ids:
                                dangling_count += 1
                    else:
                        indexed_doc_ids.add(value)
                        if value not in doc_ids:
                            dangling_count += 1

                if dangling_count > 0:
                    report['inconsistent_indexes'] += 1
                    report['dangling_entries'] += dangling_count
                    report['details'].append({
                        'collection': collection,
                        'index': idx.field,
                        'dangling_entries': dangling_count,
                    })

                for doc in all_docs:
                    key = doc.get_field(idx.field)
                    if key is not None:
                        try:
                            indexed = self.index_manager.query_by_index(
                                collection, idx.field, key
                            )
                            if doc.doc_id not in indexed:
                                report['orphaned_docs'] += 1
                                report['details'].append({
                                    'type': 'orphaned_doc',
                                    'collection': collection,
                                    'doc_id': doc.doc_id,
                                    'field': idx.field,
                                    'value': key,
                                })
                        except Exception:
                            pass

        return report

    def _fix_inconsistencies(self, report: Dict[str, Any]) -> int:
        fixed = 0

        for detail in report.get('details', []):
            if 'dangling_entries' in detail:
                collection = detail['collection']
                field = detail['index']
                fixed += self._rebuild_index(collection, field)
            elif detail.get('type') == 'orphaned_doc':
                collection = detail['collection']
                doc_id = detail['doc_id']
                field = detail['field']
                fixed += self._add_missing_index_entry(collection, doc_id, field)

        return fixed

    def _rebuild_index(self, collection: str, field: str) -> int:
        try:
            store = self._document_stores.get(collection)
            if not store:
                return 0

            all_docs = store.get_all()
            self.index_manager.rebuild_index(collection, field, all_docs)
            return 1
        except Exception:
            return 0

    def _add_missing_index_entry(self, collection: str, doc_id: DocumentID,
                                  field: str) -> int:
        try:
            store = self._document_stores.get(collection)
            if not store:
                return 0

            doc = store.get(doc_id)
            idx = self.index_manager.get_index(collection, field)
            if not idx or not idx.btree:
                return 0

            key = doc.get_field(field)
            if key is None:
                return 0

            existing = idx.btree.search(key)
            if existing is None:
                if idx.unique:
                    idx.btree.insert(key, doc_id)
                else:
                    idx.btree.insert(key, [doc_id])
            elif isinstance(existing, list) and doc_id not in existing:
                existing.append(doc_id)
                idx.btree._save_to_disk()

            return 1
        except Exception:
            return 0

    def _save_recovery_stats(self, stats: RecoveryStats) -> None:
        try:
            with open(self._recovery_stats_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'last_recovery': stats.to_dict(),
                    'timestamp': time.time(),
                }, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def get_last_recovery_stats(self) -> Optional[Dict[str, Any]]:
        try:
            if os.path.exists(self._recovery_stats_file):
                with open(self._recovery_stats_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception:
            pass
        return None
