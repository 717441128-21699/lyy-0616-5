from typing import Generic, TypeVar, List, Optional, Tuple, Any
from dataclasses import dataclass, field
import os
import struct
import pickle
import threading

K = TypeVar('K')
V = TypeVar('V')

@dataclass
class BTreeNode(Generic[K, V]):
    keys: List[K] = field(default_factory=list)
    values: List[V] = field(default_factory=list)
    children: List[int] = field(default_factory=list)
    is_leaf: bool = True
    node_id: int = -1

    def is_full(self, order: int) -> bool:
        return len(self.keys) >= order - 1

    def split(self, order: int) -> Tuple['BTreeNode[K, V]', K, V]:
        mid = (order - 1) // 2
        mid_key = self.keys[mid]
        mid_value = self.values[mid]
        new_node = BTreeNode[K, V](
            keys=self.keys[mid + 1:],
            values=self.values[mid + 1:],
            children=self.children[mid + 1:] if not self.is_leaf else [],
            is_leaf=self.is_leaf,
        )
        self.keys = self.keys[:mid]
        self.values = self.values[:mid]
        if not self.is_leaf:
            self.children = self.children[:mid + 1]
        return new_node, mid_key, mid_value


class BTree(Generic[K, V]):
    def __init__(self, order: int, file_path: str):
        self.order = order
        self.file_path = file_path
        self._lock = threading.RLock()

        self.nodes: dict[int, BTreeNode[K, V]] = {}
        self._next_node_id = 0
        self.root_id = 0
        self._height = 1

        self._initialize()

    def _initialize(self) -> None:
        if os.path.exists(self.file_path):
            self._load_from_disk()
        else:
            root = BTreeNode[K, V](node_id=0, is_leaf=True)
            self.nodes[0] = root
            self._next_node_id = 1
            self._save_to_disk()

    def _save_to_disk(self) -> None:
        with open(self.file_path, 'wb') as f:
            pickle.dump({
                'order': self.order,
                'next_node_id': self._next_node_id,
                'root_id': self.root_id,
                'height': self._height,
                'nodes': self.nodes,
            }, f)

    def _load_from_disk(self) -> None:
        with open(self.file_path, 'rb') as f:
            data = pickle.load(f)
            self.order = data['order']
            self._next_node_id = data['next_node_id']
            self.root_id = data['root_id']
            self._height = data['height']
            self.nodes = data['nodes']

    def _get_node(self, node_id: int) -> BTreeNode[K, V]:
        return self.nodes[node_id]

    def _create_node(self, is_leaf: bool) -> BTreeNode[K, V]:
        node = BTreeNode[K, V](node_id=self._next_node_id, is_leaf=is_leaf)
        self.nodes[self._next_node_id] = node
        self._next_node_id += 1
        return node

    def insert(self, key: K, value: V) -> None:
        with self._lock:
            root = self._get_node(self.root_id)

            if root.is_full(self.order):
                new_root = self._create_node(is_leaf=False)
                new_root.children.append(self.root_id)
                self._split_child(new_root, 0)
                self.root_id = new_root.node_id
                self._height += 1
                self._insert_non_full(new_root, key, value)
            else:
                self._insert_non_full(root, key, value)

            self._save_to_disk()

    def _split_child(self, parent: BTreeNode[K, V], child_idx: int) -> None:
        child = self._get_node(parent.children[child_idx])
        new_child, mid_key, mid_value = child.split(self.order)
        new_child.node_id = self._next_node_id
        self.nodes[self._next_node_id] = new_child
        self._next_node_id += 1

        parent.keys.insert(child_idx, mid_key)
        parent.values.insert(child_idx, mid_value)
        parent.children.insert(child_idx + 1, new_child.node_id)

    def _insert_non_full(self, node: BTreeNode[K, V], key: K, value: V) -> None:
        i = len(node.keys) - 1

        if node.is_leaf:
            while i >= 0 and key < node.keys[i]:
                i -= 1
            if i >= 0 and key == node.keys[i]:
                if isinstance(node.values[i], list):
                    if value not in node.values[i]:
                        node.values[i].append(value)
                else:
                    node.values[i] = value
            else:
                node.keys.insert(i + 1, key)
                node.values.insert(i + 1, value)
        else:
            while i >= 0 and key < node.keys[i]:
                i -= 1
            i += 1

            child = self._get_node(node.children[i])
            if child.is_full(self.order):
                self._split_child(node, i)
                if key > node.keys[i]:
                    i += 1

            self._insert_non_full(self._get_node(node.children[i]), key, value)

    def search(self, key: K) -> Optional[V]:
        with self._lock:
            return self._search_recursive(self._get_node(self.root_id), key)

    def _search_recursive(self, node: BTreeNode[K, V], key: K) -> Optional[V]:
        i = 0
        while i < len(node.keys) and key > node.keys[i]:
            i += 1

        if i < len(node.keys) and key == node.keys[i]:
            return node.values[i]

        if node.is_leaf:
            return None

        return self._search_recursive(self._get_node(node.children[i]), key)

    def search_range(self, min_key: K, max_key: K, include_min: bool = True,
                     include_max: bool = True) -> List[Tuple[K, V]]:
        with self._lock:
            results: List[Tuple[K, V]] = []
            self._search_range_recursive(
                self._get_node(self.root_id),
                min_key, max_key,
                include_min, include_max,
                results
            )
            return results

    def _search_range_recursive(self, node: BTreeNode[K, V],
                                min_key: K, max_key: K,
                                include_min: bool, include_max: bool,
                                results: List[Tuple[K, V]]) -> None:
        i = 0
        while i < len(node.keys):
            key = node.keys[i]

            if not node.is_leaf:
                if (include_min and key >= min_key) or (not include_min and key > min_key):
                    self._search_range_recursive(
                        self._get_node(node.children[i]),
                        min_key, max_key,
                        include_min, include_max,
                        results
                    )

            if ((include_min and key >= min_key) or (not include_min and key > min_key)) and \
               ((include_max and key <= max_key) or (not include_max and key < max_key)):
                results.append((key, node.values[i]))

            i += 1

        if not node.is_leaf:
            self._search_range_recursive(
                self._get_node(node.children[i]),
                min_key, max_key,
                include_min, include_max,
                results
            )

    def delete(self, key: K, value: Optional[V] = None) -> bool:
        with self._lock:
            result = self._delete_recursive(self._get_node(self.root_id), key, value)
            if result:
                self._save_to_disk()
            return result

    def _delete_recursive(self, node: BTreeNode[K, V], key: K,
                          value: Optional[V] = None) -> bool:
        t = (self.order + 1) // 2
        i = 0

        while i < len(node.keys) and key > node.keys[i]:
            i += 1

        if i < len(node.keys) and key == node.keys[i]:
            if node.is_leaf:
                if value is not None and isinstance(node.values[i], list):
                    if value in node.values[i]:
                        node.values[i].remove(value)
                        if not node.values[i]:
                            node.keys.pop(i)
                            node.values.pop(i)
                    else:
                        return False
                else:
                    node.keys.pop(i)
                    node.values.pop(i)
                return True
            else:
                return self._delete_from_internal(node, i, key, value)

        if node.is_leaf:
            return False

        child = self._get_node(node.children[i])
        if len(child.keys) < t:
            self._fill(node, i)
            if i > len(node.keys):
                i -= 1

        return self._delete_recursive(self._get_node(node.children[i]), key, value)

    def _delete_from_internal(self, node: BTreeNode[K, V], idx: int,
                              key: K, value: Optional[V] = None) -> bool:
        t = (self.order + 1) // 2
        child = self._get_node(node.children[idx])

        if len(child.keys) >= t:
            if value is not None and isinstance(node.values[idx], list):
                if value in node.values[idx]:
                    node.values[idx].remove(value)
                    if not node.values[idx]:
                        pred_key, pred_val = self._get_predecessor(node, idx)
                        node.keys[idx] = pred_key
                        node.values[idx] = pred_val
                        return True
                    return True
            else:
                pred_key, pred_val = self._get_predecessor(node, idx)
                node.keys[idx] = pred_key
                node.values[idx] = pred_val
                return True

        right_child = self._get_node(node.children[idx + 1])
        if len(right_child.keys) >= t:
            if value is not None and isinstance(node.values[idx], list):
                if value in node.values[idx]:
                    node.values[idx].remove(value)
                    if not node.values[idx]:
                        succ_key, succ_val = self._get_successor(node, idx)
                        node.keys[idx] = succ_key
                        node.values[idx] = succ_val
                        return True
                    return True
            else:
                succ_key, succ_val = self._get_successor(node, idx)
                node.keys[idx] = succ_key
                node.values[idx] = succ_val
                return True

        if value is not None and isinstance(node.values[idx], list):
            if value in node.values[idx]:
                node.values[idx].remove(value)
                if not node.values[idx]:
                    self._merge(node, idx)
                    return True
                return True
        else:
            self._merge(node, idx)
            return True

    def _get_predecessor(self, node: BTreeNode[K, V], idx: int) -> Tuple[K, V]:
        current = self._get_node(node.children[idx])
        while not current.is_leaf:
            current = self._get_node(current.children[-1])
        return current.keys[-1], current.values[-1]

    def _get_successor(self, node: BTreeNode[K, V], idx: int) -> Tuple[K, V]:
        current = self._get_node(node.children[idx + 1])
        while not current.is_leaf:
            current = self._get_node(current.children[0])
        return current.keys[0], current.values[0]

    def _fill(self, node: BTreeNode[K, V], idx: int) -> None:
        t = (self.order + 1) // 2

        if idx != 0 and len(self._get_node(node.children[idx - 1]).keys) >= t:
            self._borrow_from_prev(node, idx)
        elif idx != len(node.children) - 1 and \
                len(self._get_node(node.children[idx + 1]).keys) >= t:
            self._borrow_from_next(node, idx)
        else:
            if idx != len(node.children) - 1:
                self._merge(node, idx)
            else:
                self._merge(node, idx - 1)

    def _borrow_from_prev(self, node: BTreeNode[K, V], idx: int) -> None:
        child = self._get_node(node.children[idx])
        sibling = self._get_node(node.children[idx - 1])

        child.keys.insert(0, node.keys[idx - 1])
        child.values.insert(0, node.values[idx - 1])

        if not child.is_leaf:
            child.children.insert(0, sibling.children[-1])
            sibling.children.pop()

        node.keys[idx - 1] = sibling.keys[-1]
        node.values[idx - 1] = sibling.values[-1]

        sibling.keys.pop()
        sibling.values.pop()

    def _borrow_from_next(self, node: BTreeNode[K, V], idx: int) -> None:
        child = self._get_node(node.children[idx])
        sibling = self._get_node(node.children[idx + 1])

        child.keys.append(node.keys[idx])
        child.values.append(node.values[idx])

        if not child.is_leaf:
            child.children.append(sibling.children[0])
            sibling.children.pop(0)

        node.keys[idx] = sibling.keys[0]
        node.values[idx] = sibling.values[0]

        sibling.keys.pop(0)
        sibling.values.pop(0)

    def _merge(self, node: BTreeNode[K, V], idx: int) -> None:
        child = self._get_node(node.children[idx])
        sibling = self._get_node(node.children[idx + 1])

        child.keys.append(node.keys[idx])
        child.values.append(node.values[idx])

        child.keys.extend(sibling.keys)
        child.values.extend(sibling.values)

        if not child.is_leaf:
            child.children.extend(sibling.children)

        node.keys.pop(idx)
        node.values.pop(idx)
        node.children.pop(idx + 1)

        del self.nodes[sibling.node_id]

        if node.node_id == self.root_id and len(node.keys) == 0:
            self.root_id = child.node_id
            self._height -= 1

    def get_all(self) -> List[Tuple[K, V]]:
        with self._lock:
            results: List[Tuple[K, V]] = []
            self._in_order_traversal(self._get_node(self.root_id), results)
            return results

    def _in_order_traversal(self, node: BTreeNode[K, V],
                            results: List[Tuple[K, V]]) -> None:
        if node.is_leaf:
            for i in range(len(node.keys)):
                results.append((node.keys[i], node.values[i]))
        else:
            for i in range(len(node.keys)):
                self._in_order_traversal(self._get_node(node.children[i]), results)
                results.append((node.keys[i], node.values[i]))
            self._in_order_traversal(self._get_node(node.children[-1]), results)

    def count(self) -> int:
        return len(self.get_all())

    def clear(self) -> None:
        with self._lock:
            self.nodes = {}
            root = BTreeNode[K, V](node_id=0, is_leaf=True)
            self.nodes[0] = root
            self._next_node_id = 1
            self.root_id = 0
            self._height = 1
            self._save_to_disk()
