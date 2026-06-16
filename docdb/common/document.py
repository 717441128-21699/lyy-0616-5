import json
from typing import Dict, Any, Optional, Union
from dataclasses import dataclass, field
import uuid
import time

DocumentID = str

@dataclass
class Document:
    doc_id: DocumentID
    data: Dict[str, Any]
    version: int = 0
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    @staticmethod
    def generate_id() -> DocumentID:
        return str(uuid.uuid4())

    def to_json(self) -> str:
        return json.dumps({
            'doc_id': self.doc_id,
            'data': self.data,
            'version': self.version,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
        }, ensure_ascii=False)

    @staticmethod
    def from_json(json_str: str) -> 'Document':
        obj = json.loads(json_str)
        return Document(
            doc_id=obj['doc_id'],
            data=obj['data'],
            version=obj['version'],
            created_at=obj['created_at'],
            updated_at=obj['updated_at'],
        )

    def update_data(self, new_data: Dict[str, Any]) -> None:
        self.data.update(new_data)
        self.version += 1
        self.updated_at = time.time()

    def get_field(self, field_path: str) -> Optional[Any]:
        parts = field_path.split('.')
        value: Any = self.data
        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return None
        return value

    def to_bytes(self) -> bytes:
        return self.to_json().encode('utf-8')

    @staticmethod
    def from_bytes(data: bytes) -> 'Document':
        return Document.from_json(data.decode('utf-8'))
