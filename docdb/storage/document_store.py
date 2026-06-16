import os
import json
import struct
from typing import Dict, Optional, List, Tuple, Any
from collections import OrderedDict
import threading

from ..common import Document, DocumentID, Config, DocumentNotFoundError


class DocumentStore:
    def __init__(self, config: Config, collection: str):
        self.config = config
        self.collection = collection
        self.collection_dir = os.path.join(config.document_dir, collection)
        self.metadata_file = os.path.join(self.collection_dir, "_metadata.json")
        self.data_file = os.path.join(self.collection_dir, "documents.dat")
        self.index_file = os.path.join(self.collection_dir, "doc_index.dat")

        self._lock = threading.RLock()
        self._cache: OrderedDict[DocumentID, Document] = OrderedDict()
        self._cache_size = 1000

        self._doc_index: Dict[DocumentID, Tuple[int, int]] = {}

        self._initialize()

    def _initialize(self) -> None:
        os.makedirs(self.collection_dir, exist_ok=True)

        if os.path.exists(self.index_file):
            self._load_doc_index()
        else:
            self._save_doc_index()

        if not os.path.exists(self.metadata_file):
            self._save_metadata({"next_doc_id": 0, "doc_count": 0})

    def _load_doc_index(self) -> None:
        if not os.path.exists(self.index_file):
            return

        with open(self.index_file, 'rb') as f:
            data = f.read()

        offset = 0
        while offset < len(data):
            id_len = struct.unpack_from('<I', data, offset)[0]
            offset += 4
            doc_id = data[offset:offset + id_len].decode('utf-8')
            offset += id_len
            file_offset = struct.unpack_from('<Q', data, offset)[0]
            offset += 8
            data_len = struct.unpack_from('<I', data, offset)[0]
            offset += 4
            self._doc_index[doc_id] = (file_offset, data_len)

    def _save_doc_index(self) -> None:
        with open(self.index_file, 'wb') as f:
            for doc_id, (file_offset, data_len) in self._doc_index.items():
                doc_id_bytes = doc_id.encode('utf-8')
                f.write(struct.pack('<I', len(doc_id_bytes)))
                f.write(doc_id_bytes)
                f.write(struct.pack('<Q', file_offset))
                f.write(struct.pack('<I', data_len))

    def _load_metadata(self) -> Dict[str, Any]:
        with open(self.metadata_file, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _save_metadata(self, metadata: Dict[str, Any]) -> None:
        with open(self.metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)

    def _get_doc_from_cache(self, doc_id: DocumentID) -> Optional[Document]:
        if doc_id in self._cache:
            self._cache.move_to_end(doc_id)
            return self._cache[doc_id]
        return None

    def _put_doc_in_cache(self, doc: Document) -> None:
        self._cache[doc.doc_id] = doc
        self._cache.move_to_end(doc.doc_id)
        while len(self._cache) > self._cache_size:
            self._cache.popitem(last=False)

    def _remove_from_cache(self, doc_id: DocumentID) -> None:
        if doc_id in self._cache:
            del self._cache[doc_id]

    def get(self, doc_id: DocumentID) -> Document:
        with self._lock:
            cached = self._get_doc_from_cache(doc_id)
            if cached:
                return cached

            if doc_id not in self._doc_index:
                raise DocumentNotFoundError(doc_id)

            file_offset, data_len = self._doc_index[doc_id]
            with open(self.data_file, 'rb') as f:
                f.seek(file_offset)
                doc_bytes = f.read(data_len)

            doc = Document.from_bytes(doc_bytes)
            self._put_doc_in_cache(doc)
            return doc

    def get_all(self) -> List[Document]:
        with self._lock:
            documents = []
            for doc_id in list(self._doc_index.keys()):
                try:
                    documents.append(self.get(doc_id))
                except DocumentNotFoundError:
                    continue
            return documents

    def insert(self, doc: Document) -> Document:
        with self._lock:
            doc_bytes = doc.to_bytes()
            data_len = len(doc_bytes)

            file_offset = 0
            if os.path.exists(self.data_file):
                file_offset = os.path.getsize(self.data_file)

            with open(self.data_file, 'ab') as f:
                f.write(doc_bytes)

            self._doc_index[doc.doc_id] = (file_offset, data_len)
            self._save_doc_index()

            metadata = self._load_metadata()
            metadata['doc_count'] = metadata.get('doc_count', 0) + 1
            self._save_metadata(metadata)

            self._put_doc_in_cache(doc)
            return doc

    def update(self, doc_id: DocumentID, new_data: Dict[str, Any]) -> Document:
        with self._lock:
            doc = self.get(doc_id)
            old_values = {}
            for field in new_data:
                old_values[field] = doc.get_field(field)

            doc.update_data(new_data)
            self._rewrite_document(doc)
            self._put_doc_in_cache(doc)

            return doc, old_values

    def _rewrite_document(self, doc: Document) -> None:
        doc_bytes = doc.to_bytes()
        data_len = len(doc_bytes)

        old_offset, old_len = self._doc_index[doc.doc_id]
        tombstone = b'\x00' * old_len
        with open(self.data_file, 'r+b') as f:
            f.seek(old_offset)
            f.write(tombstone)

        file_offset = os.path.getsize(self.data_file)
        with open(self.data_file, 'ab') as f:
            f.write(doc_bytes)

        self._doc_index[doc.doc_id] = (file_offset, data_len)
        self._save_doc_index()

    def delete(self, doc_id: DocumentID) -> Document:
        with self._lock:
            doc = self.get(doc_id)

            file_offset, data_len = self._doc_index[doc_id]
            tombstone = b'\x00' * data_len
            with open(self.data_file, 'r+b') as f:
                f.seek(file_offset)
                f.write(tombstone)

            del self._doc_index[doc_id]
            self._save_doc_index()

            self._remove_from_cache(doc_id)

            metadata = self._load_metadata()
            metadata['doc_count'] = metadata.get('doc_count', 1) - 1
            self._save_metadata(metadata)

            return doc

    def exists(self, doc_id: DocumentID) -> bool:
        with self._lock:
            return doc_id in self._doc_index

    def count(self) -> int:
        with self._lock:
            metadata = self._load_metadata()
            return metadata.get('doc_count', 0)

    def iterate(self, batch_size: int = 100):
        doc_ids = list(self._doc_index.keys())
        for i in range(0, len(doc_ids), batch_size):
            batch = doc_ids[i:i + batch_size]
            for doc_id in batch:
                try:
                    yield self.get(doc_id)
                except DocumentNotFoundError:
                    continue
