import os
import json
import threading
from typing import Dict, List, Optional, Any, Tuple, ContextManager, Union
from dataclasses import dataclass, field
from contextlib import contextmanager

from ..common import Config, Document, DocumentID, \
    DocumentNotFoundError, IndexNotFoundError, \
    UniqueConstraintViolationError, TransactionAbortedError
from ..storage import DocumentStore
from ..index import IndexManager, SecondaryIndex
from ..wal import WriteAheadLog
from ..concurrency import LockManager
from ..transaction import TransactionManager, Transaction, IsolationLevel
from ..query import QueryOptimizer, QueryPlan
from ..recovery import RecoveryManager


class Collection:
    def __init__(self, name: str, db: 'DocDB'):
        self.name = name
        self.db = db
        self._store = db._document_stores[name]

    def insert_one(self, data: Dict[str, Any]) -> Document:
        with self.db.transaction() as txn:
            doc = self.db._transaction_manager.insert_document(txn, self.name, data)
            self.db._transaction_manager.commit_transaction(txn)
            return doc

    def insert_many(self, data_list: List[Dict[str, Any]]) -> List[Document]:
        with self.db.transaction() as txn:
            docs = []
            for data in data_list:
                doc = self.db._transaction_manager.insert_document(txn, self.name, data)
                docs.append(doc)
            self.db._transaction_manager.commit_transaction(txn)
            return docs

    def find_one(self, doc_id: DocumentID) -> Optional[Document]:
        try:
            with self.db.transaction() as txn:
                doc = self.db._transaction_manager.read_document(txn, self.name, doc_id)
                return doc
        except DocumentNotFoundError:
            return None

    def find(self, query: Optional[Dict[str, Any]] = None) -> List[Document]:
        with self.db.transaction() as txn:
            if query is None or not query:
                return self.db._transaction_manager.get_all_documents(txn, self.name)

            plan = self.db._query_optimizer.optimize_query(self.name, query)
            return self.db._query_optimizer.execute_query(txn, plan)

    def find_with_plan(self, query: Dict[str, Any]) -> Tuple[List[Document], QueryPlan]:
        with self.db.transaction() as txn:
            plan = self.db._query_optimizer.optimize_query(self.name, query)
            results = self.db._query_optimizer.execute_query(txn, plan)
            return results, plan

    def update_one(self, doc_id: DocumentID, updates: Dict[str, Any]) -> Optional[Document]:
        try:
            with self.db.transaction() as txn:
                doc = self.db._transaction_manager.update_document(
                    txn, self.name, doc_id, updates
                )
                self.db._transaction_manager.commit_transaction(txn)
                return doc
        except DocumentNotFoundError:
            return None

    def delete_one(self, doc_id: DocumentID) -> bool:
        try:
            with self.db.transaction() as txn:
                self.db._transaction_manager.delete_document(txn, self.name, doc_id)
                self.db._transaction_manager.commit_transaction(txn)
                return True
        except DocumentNotFoundError:
            return False

    def count(self) -> int:
        return self._store.count()

    def create_index(self, field: str, unique: bool = False) -> SecondaryIndex:
        existing_docs = self._store.get_all()
        return self.db._index_manager.create_index(
            self.name, field, unique, existing_docs
        )

    def drop_index(self, field: str) -> None:
        self.db._index_manager.drop_index(self.name, field)

    def list_indexes(self) -> List[SecondaryIndex]:
        return self.db._index_manager.list_indexes(self.name)

    def explain(self, query: Dict[str, Any]) -> Dict[str, Any]:
        return self.db._query_optimizer.explain(self.name, query)


class DocDB:
    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self.config.ensure_dirs()

        self._lock = threading.RLock()
        self._document_stores: Dict[str, DocumentStore] = {}
        self._collections: Dict[str, Collection] = {}
        self._collections_meta_file = os.path.join(self.config.data_dir, "_collections.json")

        self._wal = WriteAheadLog(self.config)
        self._lock_manager = LockManager(self.config)
        self._index_manager = IndexManager(self.config)
        self._transaction_manager = TransactionManager(
            self.config, self._wal, self._lock_manager, self._index_manager
        )
        self._query_optimizer = QueryOptimizer(
            self.config, self._index_manager, self._transaction_manager
        )
        self._recovery_manager = RecoveryManager(
            self.config, self._wal, self._index_manager,
            self._lock_manager, self._transaction_manager
        )

        self._initialize()

    def _initialize(self) -> None:
        self._load_collections()

        for collection_name in self._collections:
            store = DocumentStore(self.config, collection_name)
            self._document_stores[collection_name] = store
            self._transaction_manager.register_collection(collection_name, store)
            self._recovery_manager.register_collection(collection_name, store)
            self._collections[collection_name] = Collection(collection_name, self)

        if self._recovery_manager.needs_recovery():
            self._recovery_manager.recover()

        self._save_collections()

    def _load_collections(self) -> None:
        if os.path.exists(self._collections_meta_file):
            with open(self._collections_meta_file, 'r', encoding='utf-8') as f:
                meta = json.load(f)
                for name in meta.get('collections', []):
                    self._collections[name] = None

    def _save_collections(self) -> None:
        meta = {
            'collections': list(self._collections.keys()),
        }
        with open(self._collections_meta_file, 'w', encoding='utf-8') as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

    def create_collection(self, name: str) -> Collection:
        with self._lock:
            if name in self._collections:
                return self._collections[name]

            store = DocumentStore(self.config, name)
            self._document_stores[name] = store
            self._transaction_manager.register_collection(name, store)
            self._recovery_manager.register_collection(name, store)

            collection = Collection(name, self)
            self._collections[name] = collection
            self._save_collections()

            return collection

    def drop_collection(self, name: str) -> None:
        with self._lock:
            if name not in self._collections:
                return

            del self._collections[name]

            if name in self._document_stores:
                del self._document_stores[name]

            indexes = self._index_manager.list_indexes(name)
            for idx in indexes:
                self._index_manager.drop_index(name, idx.field)

            import shutil
            collection_dir = os.path.join(self.config.document_dir, name)
            if os.path.exists(collection_dir):
                shutil.rmtree(collection_dir)

            self._save_collections()

    def get_collection(self, name: str) -> Collection:
        with self._lock:
            if name not in self._collections:
                return self.create_collection(name)
            return self._collections[name]

    def list_collections(self) -> List[str]:
        with self._lock:
            return list(self._collections.keys())

    def __getitem__(self, name: str) -> Collection:
        return self.get_collection(name)

    @contextmanager
    def transaction(self, isolation_level: IsolationLevel = IsolationLevel.REPEATABLE_READ) -> ContextManager[Transaction]:
        txn = self._transaction_manager.begin_transaction(isolation_level)
        try:
            yield txn
            if txn.status.name == 'ACTIVE':
                pass
        except Exception as e:
            if txn.status.name == 'ACTIVE':
                self._transaction_manager.abort_transaction(txn)
            raise
        finally:
            if txn.status.name == 'ACTIVE':
                self._transaction_manager.abort_transaction(txn)

    def commit(self, txn: Transaction) -> None:
        self._transaction_manager.commit_transaction(txn)

    def abort(self, txn: Transaction) -> None:
        self._transaction_manager.abort_transaction(txn)

    def checkpoint(self) -> int:
        return self._wal.log_checkpoint()

    def get_recovery_stats(self) -> Optional[Dict[str, Any]]:
        return self._recovery_manager.get_last_recovery_stats()

    def get_lock_info(self) -> Dict[str, Any]:
        return {
            'wait_for_graph': self._lock_manager.get_wait_for_graph(),
            'locks': self._lock_manager.get_all_locks(),
        }

    def get_active_transactions(self) -> List[Transaction]:
        return self._transaction_manager.get_active_transactions()

    def close(self) -> None:
        with self._lock:
            self._wal.log_checkpoint()
            self._wal.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
