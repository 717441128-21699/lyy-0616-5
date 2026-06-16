from dataclasses import dataclass, field
import os

@dataclass
class Config:
    data_dir: str = "./data"
    wal_dir: str = field(init=False)
    index_dir: str = field(init=False)
    document_dir: str = field(init=False)

    wal_file_size: int = 10 * 1024 * 1024
    page_size: int = 4096
    btree_order: int = 100
    lock_timeout_ms: int = 5000
    deadlock_detection_interval_ms: int = 1000

    def __post_init__(self) -> None:
        self.wal_dir = os.path.join(self.data_dir, "wal")
        self.index_dir = os.path.join(self.data_dir, "index")
        self.document_dir = os.path.join(self.data_dir, "documents")

    def ensure_dirs(self) -> None:
        for d in [self.data_dir, self.wal_dir, self.index_dir, self.document_dir]:
            os.makedirs(d, exist_ok=True)
