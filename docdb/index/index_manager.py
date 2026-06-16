import os
import json
import threading
from typing import Dict, List, Optional, Any, Tuple, Set
from dataclasses import dataclass, field

from ..common import Config, Document, DocumentID, \
    UniqueConstraintViolationError, IndexNotFoundError
from .btree import BTree


@dataclass
class SecondaryIndex:
    collection: str
    field: str
    unique: bool = False
    btree: Optional[BTree] = None
    file_path: str = ""

    def index_name(self) -> str:
        return f"{self.collection}.{self.field}"

    def extract_key(self, doc: Document) -> Any:
        return doc.get_field(self.field)


class IndexManager:
    def __init__(self, config: Config):
        self.config = config
        self._lock = threading.RLock()
        self._indexes: Dict[str, SecondaryIndex] = {}
        self._indexes_meta_file = os.path.join(config.index_dir, "_indexes.json")
        self._initialize()

    def _initialize(self) -> None:
        os.makedirs(self.config.index_dir, exist_ok=True)

        if os.path.exists(self._indexes_meta_file):
            self._load_indexes_metadata()
        else:
            self._save_indexes_metadata()

    def _load_indexes_metadata(self) -> None:
        with open(self._indexes_meta_file, 'r', encoding='utf-8') as f:
            meta = json.load(f)

        for idx_meta in meta.get('indexes', []):
            index_file = os.path.join(
                self.config.index_dir,
                f"{idx_meta['collection']}_{idx_meta['field']}.idx"
            )
            btree = BTree[Any, DocumentID](
                order=self.config.btree_order,
                file_path=index_file
            )
            idx = SecondaryIndex(
                collection=idx_meta['collection'],
                field=idx_meta['field'],
                unique=idx_meta.get('unique', False),
                btree=btree,
                file_path=index_file
            )
            self._indexes[idx.index_name()] = idx

    def _save_indexes_metadata(self) -> None:
        meta = {
            'indexes': [
                {
                    'collection': idx.collection,
                    'field': idx.field,
                    'unique': idx.unique,
                }
                for idx in self._indexes.values()
            ]
        }
        with open(self._indexes_meta_file, 'w', encoding='utf-8') as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

    def create_index(self, collection: str, field: str, unique: bool = False,
                     existing_docs: Optional[List[Document]] = None) -> SecondaryIndex:
        with self._lock:
            index_name = f"{collection}.{field}"

            if index_name in self._indexes:
                return self._indexes[index_name]

            index_file = os.path.join(
                self.config.index_dir,
                f"{collection}_{field}.idx"
            )

            if os.path.exists(index_file):
                os.remove(index_file)

            btree = BTree[Any, DocumentID](
                order=self.config.btree_order,
                file_path=index_file
            )

            idx = SecondaryIndex(
                collection=collection,
                field=field,
                unique=unique,
                btree=btree,
                file_path=index_file
            )

            if existing_docs:
                for doc in existing_docs:
                    self._insert_into_index(idx, doc, check_unique=False)

            self._indexes[index_name] = idx
            self._save_indexes_metadata()

            return idx

    def drop_index(self, collection: str, field: str) -> None:
        with self._lock:
            index_name = f"{collection}.{field}"

            if index_name not in self._indexes:
                raise IndexNotFoundError(collection, field)

            idx = self._indexes.pop(index_name)

            if os.path.exists(idx.file_path):
                os.remove(idx.file_path)

            self._save_indexes_metadata()

    def get_index(self, collection: str, field: str) -> Optional[SecondaryIndex]:
        with self._lock:
            index_name = f"{collection}.{field}"
            return self._indexes.get(index_name)

    def list_indexes(self, collection: Optional[str] = None) -> List[SecondaryIndex]:
        with self._lock:
            if collection is None:
                return list(self._indexes.values())
            return [
                idx for idx in self._indexes.values()
                if idx.collection == collection
            ]

    def has_index(self, collection: str, field: str) -> bool:
        return self.get_index(collection, field) is not None

    def _insert_into_index(self, idx: SecondaryIndex, doc: Document,
                           check_unique: bool = True) -> None:
        key = idx.extract_key(doc)
        if key is None:
            return

        if check_unique and idx.unique:
            existing = idx.btree.search(key)
            if existing is not None:
                if isinstance(existing, list):
                    if doc.doc_id not in existing and len(existing) > 0:
                        raise UniqueConstraintViolationError(
                            idx.collection, idx.field, key
                        )
                else:
                    if existing != doc.doc_id:
                        raise UniqueConstraintViolationError(
                            idx.collection, idx.field, key
                        )

        if idx.unique:
            idx.btree.insert(key, doc.doc_id)
        else:
            existing = idx.btree.search(key)
            if existing is None:
                idx.btree.insert(key, [doc.doc_id])
            elif isinstance(existing, list):
                if doc.doc_id not in existing:
                    existing.append(doc.doc_id)
                    idx.btree._save_to_disk()

    def on_document_inserted(self, collection: str, doc: Document) -> None:
        with self._lock:
            for idx in self._indexes.values():
                if idx.collection == collection:
                    self._insert_into_index(idx, doc)

    def on_document_updated(self, collection: str, doc: Document,
                            old_values: Dict[str, Any]) -> None:
        """
        核心方法：文档更新时同步维护索引
        策略：
        1. 找出所有索引字段中值发生变化的字段
        2. 对于变化的字段：先删除旧值的索引条目，再插入新值的索引条目
        3. 整个操作在锁保护下进行，保证原子性
        """
        with self._lock:
            for idx in self._indexes.values():
                if idx.collection != collection:
                    continue

                field = idx.field
                if field not in old_values:
                    continue

                old_value = old_values[field]
                new_value = doc.get_field(field)

                if old_value == new_value:
                    continue

                if old_value is not None:
                    idx.btree.delete(old_value, doc.doc_id)

                if new_value is not None:
                    if idx.unique:
                        existing = idx.btree.search(new_value)
                        if existing is not None:
                            if isinstance(existing, list):
                                if doc.doc_id not in existing and len(existing) > 0:
                                    raise UniqueConstraintViolationError(
                                        collection, field, new_value
                                    )
                            elif existing != doc.doc_id:
                                raise UniqueConstraintViolationError(
                                    collection, field, new_value
                                )
                        idx.btree.insert(new_value, doc.doc_id)
                    else:
                        existing_list = idx.btree.search(new_value)
                        if existing_list is None:
                            idx.btree.insert(new_value, [doc.doc_id])
                        elif isinstance(existing_list, list):
                            if doc.doc_id not in existing_list:
                                existing_list.append(doc.doc_id)
                                idx.btree._save_to_disk()

    def on_document_deleted(self, collection: str, doc: Document) -> None:
        with self._lock:
            for idx in self._indexes.values():
                if idx.collection != collection:
                    continue

                key = idx.extract_key(doc)
                if key is not None:
                    idx.btree.delete(key, doc.doc_id)

    def query_by_index(self, collection: str, field: str, value: Any) -> List[DocumentID]:
        """
        使用二级索引查询文档ID
        返回匹配的文档ID列表
        """
        idx = self.get_index(collection, field)
        if idx is None:
            raise IndexNotFoundError(collection, field)

        result = idx.btree.search(value)
        if result is None:
            return []
        elif isinstance(result, list):
            return result.copy()
        else:
            return [result]

    def query_range_by_index(self, collection: str, field: str,
                             min_value: Any, max_value: Any,
                             include_min: bool = True,
                             include_max: bool = True) -> List[DocumentID]:
        """
        使用二级索引进行范围查询
        返回匹配的文档ID列表（去重）
        """
        idx = self.get_index(collection, field)
        if idx is None:
            raise IndexNotFoundError(collection, field)

        results = idx.btree.search_range(
            min_value, max_value, include_min, include_max
        )

        doc_ids: Set[DocumentID] = set()
        for _, value in results:
            if isinstance(value, list):
                doc_ids.update(value)
            else:
                doc_ids.add(value)

        return list(doc_ids)

    def estimate_index_selectivity(self, collection: str, field: str) -> float:
        """
        估计索引的选择性（区分度）
        选择性 = 唯一键数量 / 总文档数
        返回值在 0-1 之间，越接近1表示选择性越好
        """
        idx = self.get_index(collection, field)
        if idx is None:
            return 0.0

        all_entries = idx.btree.get_all()
        if not all_entries:
            return 0.0

        unique_keys = len(all_entries)
        total_docs = 0
        for _, value in all_entries:
            if isinstance(value, list):
                total_docs += len(value)
            else:
                total_docs += 1

        if total_docs == 0:
            return 0.0

        return unique_keys / total_docs

    def get_index_stats(self, collection: str, field: str) -> Dict[str, Any]:
        """
        获取索引统计信息，用于查询优化器决策
        """
        idx = self.get_index(collection, field)
        if idx is None:
            return {}

        all_entries = idx.btree.get_all()
        unique_keys = len(all_entries)
        total_docs = 0
        value_counts = []

        for _, value in all_entries:
            if isinstance(value, list):
                count = len(value)
                total_docs += count
                value_counts.append(count)
            else:
                total_docs += 1
                value_counts.append(1)

        return {
            'unique_keys': unique_keys,
            'total_docs': total_docs,
            'selectivity': unique_keys / total_docs if total_docs > 0 else 0,
            'avg_values_per_key': total_docs / unique_keys if unique_keys > 0 else 0,
            'max_values_per_key': max(value_counts) if value_counts else 0,
        }

    def rebuild_index(self, collection: str, field: str,
                      docs: List[Document]) -> None:
        """
        重建索引（用于崩溃恢复后的一致性检查）
        """
        idx = self.get_index(collection, field)
        if idx is None:
            raise IndexNotFoundError(collection, field)

        with self._lock:
            idx.btree.clear()

            for doc in docs:
                self._insert_into_index(idx, doc, check_unique=False)

    def clear_all_indexes(self) -> None:
        with self._lock:
            for idx in self._indexes.values():
                idx.btree.clear()
