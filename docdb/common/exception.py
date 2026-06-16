class DocDBException(Exception):
    pass

class DocumentNotFoundError(DocDBException):
    def __init__(self, doc_id: str):
        super().__init__(f"Document not found: {doc_id}")
        self.doc_id = doc_id

class TransactionAbortedError(DocDBException):
    def __init__(self, txn_id: int, reason: str = ""):
        super().__init__(f"Transaction {txn_id} aborted: {reason}")
        self.txn_id = txn_id
        self.reason = reason

class DeadlockError(DocDBException):
    def __init__(self, txn_id: int, victim_txn_id: int):
        super().__init__(
            f"Deadlock detected. Transaction {txn_id} aborted "
            f"(victim: {victim_txn_id})"
        )
        self.txn_id = txn_id
        self.victim_txn_id = victim_txn_id

class IndexNotFoundError(DocDBException):
    def __init__(self, collection: str, field: str):
        super().__init__(f"Index not found for {collection}.{field}")
        self.collection = collection
        self.field = field

class UniqueConstraintViolationError(DocDBException):
    def __init__(self, collection: str, field: str, value: Any):
        super().__init__(
            f"Unique constraint violation on {collection}.{field}={value}"
        )
        self.collection = collection
        self.field = field
        self.value = value

class LockTimeoutError(DocDBException):
    def __init__(self, txn_id: int, resource: str):
        super().__init__(
            f"Transaction {txn_id} timed out waiting for lock on {resource}"
        )
        self.txn_id = txn_id
        self.resource = resource
