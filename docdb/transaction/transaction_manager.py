import threading
import time
from typing import Dict, List, Optional, Any, Tuple, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import OrderedDict

from ..common import Config, Document, DocumentID, \
    TransactionAbortedError, DocumentNotFoundError, \
    UniqueConstraintViolationError
from ..storage import DocumentStore
from ..index import IndexManager
from ..wal import WriteAheadLog, WALOperation
from ..concurrency import LockManager, LockType


class TransactionStatus(Enum):
    ACTIVE = 1
    COMMITTED = 2
    ABORTED = 3


class IsolationLevel(Enum):
    READ_UNCOMMITTED = 1
    READ_COMMITTED = 2
    REPEATABLE_READ = 3
    SERIALIZABLE = 4


@dataclass
class Transaction:
    txn_id: int
    status: TransactionStatus
    isolation_level: IsolationLevel
    start_time: float
    commit_time: Optional[float] = None
    undo_log: List[Callable] = field(default_factory=list)
    write_set: Dict[str, Document] = field(default_factory=dict)
    read_set: Dict[str, Document] = field(default_factory=dict)

    def get_write_key(self, collection: str, doc_id: DocumentID) -> str:
        return f"{collection}:{doc_id}"

    def add_to_write_set(self, collection: str, doc: Document) -> None:
        key = self.get_write_key(collection, doc.doc_id)
        self.write_set[key] = doc

    def get_from_write_set(self, collection: str, doc_id: DocumentID) -> Optional[Document]:
        key = self.get_write_key(collection, doc_id)
        return self.write_set.get(key)

    def add_to_read_set(self, collection: str, doc: Document) -> None:
        key = self.get_write_key(collection, doc.doc_id)
        if key not in self.read_set:
            self.read_set[key] = doc

    def add_undo_action(self, action: Callable) -> None:
        self.undo_log.append(action)


class TransactionManager:
    def __init__(self, config: Config, wal: WriteAheadLog,
                 lock_manager: LockManager, index_manager: IndexManager):
        self.config = config
        self.wal = wal
        self.lock_manager = lock_manager
        self.index_manager = index_manager

        self._lock = threading.RLock()
        self._transactions: Dict[int, Transaction] = {}
        self._document_stores: Dict[str, DocumentStore] = {}
        self._mvcc_store: Dict[str, OrderedDict[int, Document]] = {}
        self._committed_txns: List[int] = []

    def register_collection(self, collection: str, store: DocumentStore) -> None:
        with self._lock:
            self._document_stores[collection] = store
            if collection not in self._mvcc_store:
                self._mvcc_store[collection] = OrderedDict()

    def begin_transaction(self, isolation_level: IsolationLevel = IsolationLevel.REPEATABLE_READ) -> Transaction:
        txn_id = self.lock_manager.allocate_txn_id()
        txn = Transaction(
            txn_id=txn_id,
            status=TransactionStatus.ACTIVE,
            isolation_level=isolation_level,
            start_time=time.time(),
        )

        with self._lock:
            self._transactions[txn_id] = txn

        self.wal.begin_transaction(txn_id)
        return txn

    def _get_store(self, collection: str) -> DocumentStore:
        if collection not in self._document_stores:
            raise ValueError(f"Collection '{collection}' not found")
        return self._document_stores[collection]

    def _get_mvcc_versions(self, collection: str) -> OrderedDict[int, Document]:
        if collection not in self._mvcc_store:
            self._mvcc_store[collection] = OrderedDict()
        return self._mvcc_store[collection]

    def _get_visible_version(self, txn: Transaction, collection: str,
                             doc_id: DocumentID) -> Optional[Document]:
        if txn.isolation_level == IsolationLevel.READ_UNCOMMITTED:
            for other_txn in self._transactions.values():
                if other_txn.txn_id != txn.txn_id and \
                        other_txn.status == TransactionStatus.ACTIVE:
                    key = other_txn.get_write_key(collection, doc_id)
                    if key in other_txn.write_set:
                        return other_txn.write_set[key]

        written = txn.get_from_write_set(collection, doc_id)
        if written:
            return written

        if txn.isolation_level in (IsolationLevel.REPEATABLE_READ, IsolationLevel.SERIALIZABLE):
            key = txn.get_write_key(collection, doc_id)
            if key in txn.read_set:
                return txn.read_set[key]

        versions = self._get_mvcc_versions(collection)
        doc_key = doc_id

        committed_docs = []
        for version_txn_id, doc in versions.items():
            if doc.doc_id == doc_key and version_txn_id in self._committed_txns:
                if version_txn_id < txn.txn_id:
                    committed_docs.append((version_txn_id, doc))

        if committed_docs:
            committed_docs.sort(key=lambda x: x[0], reverse=True)
            return committed_docs[0][1]

        store = self._get_store(collection)
        try:
            doc = store.get(doc_id)
            if txn.isolation_level in (IsolationLevel.REPEATABLE_READ, IsolationLevel.SERIALIZABLE):
                txn.add_to_read_set(collection, doc)
            return doc
        except DocumentNotFoundError:
            return None

    def _acquire_document_lock(self, txn: Transaction, collection: str,
                               doc_id: DocumentID, lock_type: LockType) -> None:
        if txn.isolation_level in (IsolationLevel.READ_COMMITTED, IsolationLevel.READ_UNCOMMITTED):
            if lock_type == LockType.SHARED:
                return

        if self.lock_manager.has_lock(txn.txn_id, collection, doc_id, lock_type):
            return

        self.lock_manager.acquire_lock(txn.txn_id, collection, doc_id, lock_type)

    def read_document(self, txn: Transaction, collection: str,
                      doc_id: DocumentID) -> Document:
        if txn.status != TransactionStatus.ACTIVE:
            raise TransactionAbortedError(txn.txn_id, "Transaction not active")

        self._acquire_document_lock(txn, collection, doc_id, LockType.SHARED)

        doc = self._get_visible_version(txn, collection, doc_id)
        if doc is None:
            raise DocumentNotFoundError(doc_id)

        return doc

    def insert_document(self, txn: Transaction, collection: str,
                        data: Dict[str, Any]) -> Document:
        if txn.status != TransactionStatus.ACTIVE:
            raise TransactionAbortedError(txn.txn_id, "Transaction not active")

        doc_id = Document.generate_id()
        doc = Document(doc_id=doc_id, data=data)

        self._acquire_document_lock(txn, collection, doc_id, LockType.EXCLUSIVE)

        indexes = self.index_manager.list_indexes(collection)
        for idx in indexes:
            key = idx.extract_key(doc)
            if key is not None and idx.unique:
                existing = self.index_manager.query_by_index(collection, idx.field, key)
                if existing:
                    for other_doc_id in existing:
                        other_doc = self._get_visible_version(txn, collection, other_doc_id)
                        if other_doc and other_doc_id != doc_id:
                            raise UniqueConstraintViolationError(collection, idx.field, key)

        txn.add_to_write_set(collection, doc)
        self.wal.log_insert(txn.txn_id, collection, doc)

        def undo_insert():
            del txn.write_set[txn.get_write_key(collection, doc_id)]

        txn.add_undo_action(undo_insert)

        return doc

    def update_document(self, txn: Transaction, collection: str,
                        doc_id: DocumentID, updates: Dict[str, Any]) -> Document:
        if txn.status != TransactionStatus.ACTIVE:
            raise TransactionAbortedError(txn.txn_id, "Transaction not active")

        self._acquire_document_lock(txn, collection, doc_id, LockType.EXCLUSIVE)

        current_doc = self._get_visible_version(txn, collection, doc_id)
        if current_doc is None:
            raise DocumentNotFoundError(doc_id)

        old_doc = Document(
            doc_id=current_doc.doc_id,
            data=dict(current_doc.data),
            version=current_doc.version,
            created_at=current_doc.created_at,
            updated_at=current_doc.updated_at,
        )

        new_doc = Document(
            doc_id=current_doc.doc_id,
            data=dict(current_doc.data),
            version=current_doc.version,
            created_at=current_doc.created_at,
            updated_at=time.time(),
        )
        new_doc.update_data(updates)

        indexes = self.index_manager.list_indexes(collection)
        old_values = {}
        for idx in indexes:
            old_val = old_doc.get_field(idx.field)
            new_val = new_doc.get_field(idx.field)
            if old_val != new_val:
                old_values[idx.field] = old_val
                if idx.unique and new_val is not None:
                    existing = self.index_manager.query_by_index(collection, idx.field, new_val)
                    if existing:
                        for other_doc_id in existing:
                            if other_doc_id != doc_id:
                                other_doc = self._get_visible_version(txn, collection, other_doc_id)
                                if other_doc:
                                    raise UniqueConstraintViolationError(collection, idx.field, new_val)

        txn.add_to_write_set(collection, new_doc)
        self.wal.log_update(txn.txn_id, collection, old_doc, new_doc)

        for idx in indexes:
            if idx.field in old_values:
                old_val = old_values[idx.field]
                new_val = new_doc.get_field(idx.field)
                if old_val is not None:
                    self.wal.log_index_delete(txn.txn_id, idx.index_name(), old_val, doc_id)
                if new_val is not None:
                    self.wal.log_index_insert(txn.txn_id, idx.index_name(), new_val, doc_id)

        def undo_update():
            txn.write_set[txn.get_write_key(collection, doc_id)] = old_doc

        txn.add_undo_action(undo_update)

        return new_doc

    def delete_document(self, txn: Transaction, collection: str,
                        doc_id: DocumentID) -> Document:
        if txn.status != TransactionStatus.ACTIVE:
            raise TransactionAbortedError(txn.txn_id, "Transaction not active")

        self._acquire_document_lock(txn, collection, doc_id, LockType.EXCLUSIVE)

        doc = self._get_visible_version(txn, collection, doc_id)
        if doc is None:
            raise DocumentNotFoundError(doc_id)

        key = txn.get_write_key(collection, doc_id)
        txn.write_set[key] = None

        self.wal.log_delete(txn.txn_id, collection, doc)

        indexes = self.index_manager.list_indexes(collection)
        for idx in indexes:
            val = doc.get_field(idx.field)
            if val is not None:
                self.wal.log_index_delete(txn.txn_id, idx.index_name(), val, doc_id)

        def undo_delete():
            txn.write_set[key] = doc

        txn.add_undo_action(undo_delete)

        return doc

    def _validate_serializable(self, txn: Transaction) -> bool:
        if txn.isolation_level != IsolationLevel.SERIALIZABLE:
            return True

        for key, read_doc in txn.read_set.items():
            parts = key.split(':', 1)
            collection, doc_id = parts[0], parts[1]

            versions = self._get_mvcc_versions(collection)
            for version_txn_id, version_doc in versions.items():
                if version_doc.doc_id == doc_id and \
                        version_txn_id > txn.txn_id and \
                        version_txn_id in self._committed_txns:
                    return False

        return True

    def commit_transaction(self, txn: Transaction) -> None:
        if txn.status != TransactionStatus.ACTIVE:
            raise TransactionAbortedError(txn.txn_id, "Transaction not active")

        if not self._validate_serializable(txn):
            self.abort_transaction(txn)
            raise TransactionAbortedError(txn.txn_id, "Serialization failure")

        try:
            for key, doc in txn.write_set.items():
                parts = key.split(':', 1)
                collection, doc_id = parts[0], parts[1]
                store = self._get_store(collection)

                if doc is None:
                    existing_doc = None
                    try:
                        existing_doc = store.get(doc_id)
                        store.delete(doc_id)
                        self.index_manager.on_document_deleted(collection, existing_doc)
                    except DocumentNotFoundError:
                        pass
                else:
                    old_values = {}
                    try:
                        existing_doc = store.get(doc_id)
                        for idx in self.index_manager.list_indexes(collection):
                            old_values[idx.field] = existing_doc.get_field(idx.field)
                        doc, _ = store.update(doc_id, doc.data)
                        self.index_manager.on_document_updated(collection, doc, old_values)
                    except DocumentNotFoundError:
                        store.insert(doc)
                        self.index_manager.on_document_inserted(collection, doc)

                    versions = self._get_mvcc_versions(collection)
                    versions[txn.txn_id] = doc

            self.wal.commit_transaction(txn.txn_id)

            txn.status = TransactionStatus.COMMITTED
            txn.commit_time = time.time()

            with self._lock:
                self._committed_txns.append(txn.txn_id)

            self.lock_manager.release_all_locks(txn.txn_id)

        except Exception as e:
            self.abort_transaction(txn)
            raise

    def abort_transaction(self, txn: Transaction) -> None:
        if txn.status == TransactionStatus.ABORTED:
            return

        for action in reversed(txn.undo_log):
            try:
                action()
            except Exception:
                pass

        self.wal.abort_transaction(txn.txn_id)

        txn.status = TransactionStatus.ABORTED

        self.lock_manager.release_all_locks(txn.txn_id)

        with self._lock:
            if txn.txn_id in self._transactions:
                del self._transactions[txn.txn_id]

    def get_transaction(self, txn_id: int) -> Optional[Transaction]:
        with self._lock:
            return self._transactions.get(txn_id)

    def get_active_transactions(self) -> List[Transaction]:
        with self._lock:
            return [
                txn for txn in self._transactions.values()
                if txn.status == TransactionStatus.ACTIVE
            ]

    def cleanup_old_versions(self, min_txn_id: int) -> None:
        with self._lock:
            for collection, versions in self._mvcc_store.items():
                to_remove = []
                for txn_id in versions:
                    if txn_id < min_txn_id and txn_id in self._committed_txns:
                        to_remove.append(txn_id)

                for txn_id in to_remove:
                    del versions[txn_id]

            if min_txn_id > 0:
                self._committed_txns = [
                    t for t in self._committed_txns if t >= min_txn_id
                ]

    def get_all_documents(self, txn: Transaction, collection: str) -> List[Document]:
        store = self._get_store(collection)
        all_docs = store.get_all()

        result = []
        for doc in all_docs:
            visible = self._get_visible_version(txn, collection, doc.doc_id)
            if visible:
                result.append(visible)

        for key, doc in txn.write_set.items():
            parts = key.split(':', 1)
            if parts[0] == collection and doc is not None:
                if not any(d.doc_id == doc.doc_id for d in result):
                    result.append(doc)

        return result
