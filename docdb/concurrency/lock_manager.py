import threading
import time
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum

from ..common import Config, DocumentID, DeadlockError, LockTimeoutError, TransactionAbortedError


class LockType(Enum):
    SHARED = 1
    EXCLUSIVE = 2


@dataclass
class LockRequest:
    txn_id: int
    resource: str
    lock_type: LockType
    timestamp: float
    granted: bool = False


@dataclass
class Lock:
    resource: str
    holders: Dict[int, LockType] = field(default_factory=dict)
    waiters: List[LockRequest] = field(default_factory=list)

    def is_compatible(self, new_type: LockType) -> bool:
        if not self.holders:
            return True

        if new_type == LockType.EXCLUSIVE:
            return False

        for holder_type in self.holders.values():
            if holder_type == LockType.EXCLUSIVE:
                return False

        return True

    def can_grant(self, request: LockRequest) -> bool:
        if not self.is_compatible(request.lock_type):
            return False

        for waiter in self.waiters:
            if waiter.timestamp < request.timestamp:
                return False

        return True


class LockManager:
    def __init__(self, config: Config):
        self.config = config
        self._lock = threading.RLock()
        self._locks: Dict[str, Lock] = {}
        self._txn_locks: Dict[int, Set[str]] = {}
        self._wait_for_graph: Dict[int, Set[int]] = {}
        self._txn_start_time: Dict[int, float] = {}
        self._next_txn_id = 0

    def allocate_txn_id(self) -> int:
        with self._lock:
            self._next_txn_id += 1
            txn_id = self._next_txn_id
            self._txn_start_time[txn_id] = time.time()
            self._wait_for_graph[txn_id] = set()
            self._txn_locks[txn_id] = set()
            return txn_id

    def _get_or_create_lock(self, resource: str) -> Lock:
        if resource not in self._locks:
            self._locks[resource] = Lock(resource=resource)
        return self._locks[resource]

    @staticmethod
    def _get_resource_key(collection: str, doc_id: Optional[DocumentID] = None) -> str:
        if doc_id:
            return f"{collection}:{doc_id}"
        return f"{collection}:*"

    def acquire_lock(self, txn_id: int, collection: str,
                     doc_id: Optional[DocumentID], lock_type: LockType,
                     timeout_ms: Optional[int] = None) -> bool:
        """
        获取锁
        死锁避免策略：
        1. 按资源ID有序加锁（在事务层保证）
        2. 等待-死亡（Wait-Die）方案：旧事务等待新事务，新事务回滚
        3. 死锁检测：构建等待图，检测环
        """
        if timeout_ms is None:
            timeout_ms = self.config.lock_timeout_ms

        resource = self._get_resource_key(collection, doc_id)
        request = LockRequest(
            txn_id=txn_id,
            resource=resource,
            lock_type=lock_type,
            timestamp=self._txn_start_time.get(txn_id, time.time()),
        )

        start_time = time.time()
        deadline = start_time + timeout_ms / 1000.0

        with self._lock:
            lock = self._get_or_create_lock(resource)

            while time.time() < deadline:
                if lock.can_grant(request):
                    self._grant_lock(lock, request, txn_id)
                    return True

                if self._check_deadlock(txn_id):
                    self._abort_txn(txn_id)
                    raise DeadlockError(txn_id, self._select_victim())

                if not self._wait_die(request, lock):
                    self._abort_txn(txn_id)
                    raise TransactionAbortedError(
                        txn_id, "Wait-Die: newer transaction aborted"
                    )

                self._update_wait_for_graph(txn_id, lock)
                self._lock.release()
                time.sleep(0.001)
                self._lock.acquire()
                lock = self._get_or_create_lock(resource)

            raise LockTimeoutError(txn_id, resource)

    def _grant_lock(self, lock: Lock, request: LockRequest, txn_id: int) -> None:
        lock.holders[txn_id] = request.lock_type
        request.granted = True
        self._txn_locks[txn_id].add(lock.resource)
        self._remove_from_waiters(lock, txn_id)
        self._remove_from_wait_for_graph(txn_id)

    def _remove_from_waiters(self, lock: Lock, txn_id: int) -> None:
        lock.waiters = [w for w in lock.waiters if w.txn_id != txn_id]

    def _remove_from_wait_for_graph(self, txn_id: int) -> None:
        if txn_id in self._wait_for_graph:
            self._wait_for_graph[txn_id].clear()

        for edges in self._wait_for_graph.values():
            edges.discard(txn_id)

    def _wait_die(self, request: LockRequest, lock: Lock) -> bool:
        """
        等待-死亡（Wait-Die）死锁避免方案
        - 如果请求事务比持有锁的事务旧（timestamp更小），则等待
        - 否则，回滚（死亡）
        这是一个非抢占式方案，可以防止死锁
        """
        if not lock.holders:
            return True

        request_ts = request.timestamp
        min_holder_ts = min(
            self._txn_start_time.get(holder_id, float('inf'))
            for holder_id in lock.holders
        )

        return request_ts < min_holder_ts

    def _update_wait_for_graph(self, txn_id: int, lock: Lock) -> None:
        for holder_id in lock.holders:
            if holder_id != txn_id and holder_id in self._wait_for_graph:
                self._wait_for_graph[txn_id].add(holder_id)

    def _check_deadlock(self, txn_id: int) -> bool:
        """
        死锁检测：使用DFS检测等待图中的环
        """
        visited: Set[int] = set()
        recursion_stack: Set[int] = set()

        def dfs(current: int) -> bool:
            visited.add(current)
            recursion_stack.add(current)

            for neighbor in self._wait_for_graph.get(current, set()):
                if neighbor not in visited:
                    if dfs(neighbor):
                        return True
                elif neighbor in recursion_stack:
                    return True

            recursion_stack.discard(current)
            return False

        return dfs(txn_id)

    def _select_victim(self) -> int:
        """
        选择死锁牺牲品
        策略：选择最年轻（启动时间最晚）的事务，回滚代价最小
        """
        candidates = []
        for txn_id, edges in self._wait_for_graph.items():
            if edges:
                candidates.append(
                    (self._txn_start_time.get(txn_id, 0), txn_id)
                )

        if not candidates:
            return 0

        candidates.sort(reverse=True)
        return candidates[0][1]

    def _abort_txn(self, txn_id: int) -> None:
        """
        清理事务的所有锁和等待图条目
        """
        if txn_id in self._txn_locks:
            for resource in list(self._txn_locks[txn_id]):
                self.release_lock(txn_id, resource)
            self._txn_locks[txn_id].clear()

        self._remove_from_wait_for_graph(txn_id)

        for lock in self._locks.values():
            self._remove_from_waiters(lock, txn_id)

    def release_lock(self, txn_id: int, resource: str) -> None:
        with self._lock:
            if resource not in self._locks:
                return

            lock = self._locks[resource]

            if txn_id in lock.holders:
                del lock.holders[txn_id]

            if txn_id in self._txn_locks:
                self._txn_locks[txn_id].discard(resource)

            self._remove_from_wait_for_graph(txn_id)
            self._process_waiting_requests(lock)

    def release_all_locks(self, txn_id: int) -> None:
        with self._lock:
            if txn_id in self._txn_locks:
                resources = list(self._txn_locks[txn_id])
                for resource in resources:
                    self.release_lock(txn_id, resource)
                self._txn_locks[txn_id].clear()

            self._remove_from_wait_for_graph(txn_id)

            for lock in self._locks.values():
                self._remove_from_waiters(lock, txn_id)

            if txn_id in self._wait_for_graph:
                del self._wait_for_graph[txn_id]
            if txn_id in self._txn_start_time:
                del self._txn_start_time[txn_id]

    def _process_waiting_requests(self, lock: Lock) -> None:
        for waiter in list(lock.waiters):
            if not waiter.granted and lock.can_grant(waiter):
                self._grant_lock(lock, waiter, waiter.txn_id)

    def get_held_locks(self, txn_id: int) -> Dict[str, LockType]:
        with self._lock:
            locks = {}
            if txn_id in self._txn_locks:
                for resource in self._txn_locks[txn_id]:
                    if resource in self._locks:
                        lock = self._locks[resource]
                        if txn_id in lock.holders:
                            locks[resource] = lock.holders[txn_id]
            return locks

    def has_lock(self, txn_id: int, collection: str,
                 doc_id: Optional[DocumentID], lock_type: LockType) -> bool:
        resource = self._get_resource_key(collection, doc_id)
        with self._lock:
            if resource not in self._locks:
                return False
            lock = self._locks[resource]
            if txn_id not in lock.holders:
                return False
            held_type = lock.holders[txn_id]
            if lock_type == LockType.SHARED:
                return True
            return held_type == LockType.EXCLUSIVE

    def upgrade_lock(self, txn_id: int, collection: str,
                     doc_id: Optional[DocumentID]) -> bool:
        resource = self._get_resource_key(collection, doc_id)
        with self._lock:
            if resource not in self._locks:
                return False

            lock = self._locks[resource]

            if txn_id not in lock.holders:
                return False

            if lock.holders[txn_id] == LockType.EXCLUSIVE:
                return True

            other_holders = [
                h for h in lock.holders if h != txn_id
            ]
            if other_holders:
                return False

            lock.holders[txn_id] = LockType.EXCLUSIVE
            return True

    def get_wait_for_graph(self) -> Dict[int, List[int]]:
        with self._lock:
            return {
                txn_id: sorted(list(edges))
                for txn_id, edges in self._wait_for_graph.items()
            }

    def get_all_locks(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            result = {}
            for resource, lock in self._locks.items():
                result[resource] = {
                    'holders': {
                        str(txn_id): lock_type.name
                        for txn_id, lock_type in lock.holders.items()
                    },
                    'waiters': [
                        {
                            'txn_id': w.txn_id,
                            'type': w.lock_type.name,
                            'granted': w.granted,
                        }
                        for w in lock.waiters
                    ]
                }
            return result
