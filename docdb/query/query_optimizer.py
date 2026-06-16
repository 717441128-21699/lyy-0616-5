from typing import Dict, List, Optional, Any, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
import threading

from ..common import Config, Document, DocumentID, IndexNotFoundError
from ..index import IndexManager, SecondaryIndex
from ..transaction import TransactionManager, Transaction, IsolationLevel


class QueryType(Enum):
    FULL_SCAN = 1
    INDEX_SCAN = 2
    INDEX_RANGE_SCAN = 3
    COMPOSITE_SCAN = 4


class Operator(Enum):
    EQ = '='
    NE = '!='
    GT = '>'
    GTE = '>='
    LT = '<'
    LTE = '<='
    IN = 'in'
    NIN = 'nin'
    EXISTS = 'exists'


@dataclass
class QueryCondition:
    field: str
    operator: Operator
    value: Any

    def to_dict(self) -> Dict[str, Any]:
        return {
            'field': self.field,
            'operator': self.operator.value,
            'value': self.value,
        }


@dataclass
class QueryPlan:
    query_type: QueryType
    collection: str
    conditions: List[QueryCondition] = field(default_factory=list)
    use_index: Optional[str] = None
    index_range: Optional[Tuple[Any, Any, bool, bool]] = None
    estimated_cost: float = 0.0
    estimated_rows: int = 0
    filter_conditions: List[QueryCondition] = field(default_factory=list)

    def explain(self) -> Dict[str, Any]:
        return {
            'query_type': self.query_type.name,
            'collection': self.collection,
            'use_index': self.use_index,
            'index_range': self.index_range,
            'estimated_cost': self.estimated_cost,
            'estimated_rows': self.estimated_rows,
            'conditions': [c.to_dict() for c in self.conditions],
            'filter_conditions': [c.to_dict() for c in self.filter_conditions],
        }


@dataclass
class CostEstimate:
    full_scan_cost: float
    index_scan_cost: float
    estimated_rows_full: int
    estimated_rows_index: int
    selectivity: float
    index_name: Optional[str]


class QueryOptimizer:
    def __init__(self, config: Config, index_manager: IndexManager,
                 transaction_manager: TransactionManager):
        self.config = config
        self.index_manager = index_manager
        self.transaction_manager = transaction_manager
        self._lock = threading.RLock()

        self._full_scan_threshold_ratio = 0.3
        self._index_cost_per_row = 1.5
        self._full_scan_cost_per_row = 1.0
        self._random_io_cost = 10.0
        self._sequential_io_cost = 1.0

    def parse_query(self, query_dict: Dict[str, Any]) -> List[QueryCondition]:
        conditions = []
        for field, condition in query_dict.items():
            if isinstance(condition, dict):
                for op_str, value in condition.items():
                    op = self._parse_operator(op_str)
                    conditions.append(QueryCondition(field=field, operator=op, value=value))
            else:
                conditions.append(
                    QueryCondition(field=field, operator=Operator.EQ, value=condition)
                )
        return conditions

    def _parse_operator(self, op_str: str) -> Operator:
        op_map = {
            '$eq': Operator.EQ,
            '$ne': Operator.NE,
            '$gt': Operator.GT,
            '$gte': Operator.GTE,
            '$lt': Operator.LT,
            '$lte': Operator.LTE,
            '$in': Operator.IN,
            '$nin': Operator.NIN,
            '$exists': Operator.EXISTS,
        }
        return op_map.get(op_str, Operator.EQ)

    def _estimate_cost(self, collection: str,
                       conditions: List[QueryCondition]) -> CostEstimate:
        with self._lock:
            doc_store = self.transaction_manager._document_stores.get(collection)
            if doc_store is None:
                return CostEstimate(0, 0, 0, 0, 0, None)

            total_docs = doc_store.count()

            full_scan_cost = total_docs * self._full_scan_cost_per_row

            best_index_cost = float('inf')
            best_index_name = None
            best_estimated_rows = total_docs
            best_selectivity = 0.0

            for condition in conditions:
                if condition.operator in (Operator.EQ, Operator.GT, Operator.GTE,
                                          Operator.LT, Operator.LTE):
                    idx = self.index_manager.get_index(collection, condition.field)
                    if idx is None:
                        continue

                    stats = self.index_manager.get_index_stats(collection, condition.field)
                    selectivity = stats.get('selectivity', 0)
                    unique_keys = stats.get('unique_keys', 0)
                    total_docs_indexed = stats.get('total_docs', 0)

                    if total_docs_indexed == 0:
                        continue

                    estimated_rows = self._estimate_rows(condition, stats, total_docs)
                    index_cost = self._calculate_index_cost(
                        condition, estimated_rows, unique_keys, total_docs_indexed
                    )

                    if index_cost < best_index_cost:
                        best_index_cost = index_cost
                        best_index_name = idx.index_name()
                        best_estimated_rows = estimated_rows
                        best_selectivity = selectivity

            return CostEstimate(
                full_scan_cost=full_scan_cost,
                index_scan_cost=best_index_cost,
                estimated_rows_full=total_docs,
                estimated_rows_index=best_estimated_rows,
                selectivity=best_selectivity,
                index_name=best_index_name,
            )

    def _estimate_rows(self, condition: QueryCondition, stats: Dict[str, Any],
                       total_docs: int) -> int:
        if condition.operator == Operator.EQ:
            avg_per_key = stats.get('avg_values_per_key', 1)
            return int(avg_per_key)
        elif condition.operator in (Operator.GT, Operator.GTE, Operator.LT, Operator.LTE):
            selectivity = stats.get('selectivity', 0)
            return int(total_docs * 0.5 / max(selectivity, 0.01))
        else:
            return total_docs

    def _calculate_index_cost(self, condition: QueryCondition, estimated_rows: int,
                              unique_keys: int, total_docs: int) -> float:
        tree_height = 1
        if unique_keys > 0:
            tree_height = max(1, int(__import__('math').log2(unique_keys) / __import__('math').log2(self.config.btree_order)))

        traversal_cost = tree_height * self._random_io_cost

        if condition.operator == Operator.EQ:
            scan_cost = estimated_rows * self._index_cost_per_row * self._random_io_cost
        else:
            scan_cost = estimated_rows * self._index_cost_per_row * self._sequential_io_cost

        fetch_cost = estimated_rows * self._random_io_cost

        return traversal_cost + scan_cost + fetch_cost

    def optimize_query(self, collection: str,
                       query_dict: Dict[str, Any]) -> QueryPlan:
        conditions = self.parse_query(query_dict)

        cost_estimate = self._estimate_cost(collection, conditions)

        index_conditions = []
        filter_conditions = []
        best_index_field = None

        if cost_estimate.index_name:
            best_index_field = cost_estimate.index_name.split('.', 1)[1]

        for condition in conditions:
            if condition.field == best_index_field and \
                    condition.operator in (Operator.EQ, Operator.GT, Operator.GTE,
                                           Operator.LT, Operator.LTE):
                index_conditions.append(condition)
            else:
                filter_conditions.append(condition)

        if cost_estimate.index_name and index_conditions and \
                cost_estimate.index_scan_cost < cost_estimate.full_scan_cost * self._full_scan_threshold_ratio:

            index_range = None
            query_type = QueryType.INDEX_SCAN

            if len(index_conditions) == 1:
                cond = index_conditions[0]
                if cond.operator == Operator.EQ:
                    index_range = (cond.value, cond.value, True, True)
                elif cond.operator == Operator.GT:
                    index_range = (cond.value, None, False, False)
                elif cond.operator == Operator.GTE:
                    index_range = (cond.value, None, True, False)
                elif cond.operator == Operator.LT:
                    index_range = (None, cond.value, False, False)
                elif cond.operator == Operator.LTE:
                    index_range = (None, cond.value, False, True)
                query_type = QueryType.INDEX_RANGE_SCAN if cond.operator != Operator.EQ else QueryType.INDEX_SCAN
            elif len(index_conditions) > 1:
                min_val = None
                max_val = None
                include_min = True
                include_max = True

                for cond in index_conditions:
                    if cond.operator in (Operator.GT, Operator.GTE):
                        min_val = cond.value
                        include_min = cond.operator == Operator.GTE
                    elif cond.operator in (Operator.LT, Operator.LTE):
                        max_val = cond.value
                        include_max = cond.operator == Operator.LTE

                if min_val is not None or max_val is not None:
                    index_range = (min_val, max_val, include_min, include_max)
                    query_type = QueryType.INDEX_RANGE_SCAN

            return QueryPlan(
                query_type=query_type,
                collection=collection,
                conditions=conditions,
                use_index=cost_estimate.index_name,
                index_range=index_range,
                estimated_cost=cost_estimate.index_scan_cost,
                estimated_rows=cost_estimate.estimated_rows_index,
                filter_conditions=filter_conditions,
            )
        else:
            return QueryPlan(
                query_type=QueryType.FULL_SCAN,
                collection=collection,
                conditions=conditions,
                estimated_cost=cost_estimate.full_scan_cost,
                estimated_rows=cost_estimate.estimated_rows_full,
                filter_conditions=conditions,
            )

    def execute_query(self, txn: Transaction, plan: QueryPlan) -> List[Document]:
        if plan.query_type == QueryType.FULL_SCAN:
            return self._execute_full_scan(txn, plan)
        elif plan.query_type in (QueryType.INDEX_SCAN, QueryType.INDEX_RANGE_SCAN):
            return self._execute_index_scan(txn, plan)
        else:
            return self._execute_full_scan(txn, plan)

    def _execute_full_scan(self, txn: Transaction, plan: QueryPlan) -> List[Document]:
        all_docs = self.transaction_manager.get_all_documents(txn, plan.collection)
        return [doc for doc in all_docs if self._matches_conditions(doc, plan.filter_conditions)]

    def _execute_index_scan(self, txn: Transaction, plan: QueryPlan) -> List[Document]:
        if not plan.use_index:
            return self._execute_full_scan(txn, plan)

        parts = plan.use_index.split('.', 1)
        collection, field = parts[0], parts[1]

        doc_ids: List[DocumentID] = []

        if plan.index_range:
            min_val, max_val, include_min, include_max = plan.index_range
            if min_val is not None and max_val is not None:
                doc_ids = self.index_manager.query_range_by_index(
                    collection, field, min_val, max_val, include_min, include_max
                )
            elif min_val is not None:
                import sys
                doc_ids = self.index_manager.query_range_by_index(
                    collection, field, min_val, sys.maxsize, include_min, False
                )
            elif max_val is not None:
                doc_ids = self.index_manager.query_range_by_index(
                    collection, field, -float('inf'), max_val, False, include_max
                )
        else:
            eq_condition = None
            for cond in plan.conditions:
                if cond.field == field and cond.operator == Operator.EQ:
                    eq_condition = cond
                    break

            if eq_condition:
                doc_ids = self.index_manager.query_by_index(
                    collection, field, eq_condition.value
                )

        results = []
        for doc_id in doc_ids:
            try:
                doc = self.transaction_manager.read_document(txn, plan.collection, doc_id)
                if self._matches_conditions(doc, plan.filter_conditions):
                    results.append(doc)
            except Exception:
                continue

        return results

    def _matches_conditions(self, doc: Document,
                            conditions: List[QueryCondition]) -> bool:
        for condition in conditions:
            if not self._matches_condition(doc, condition):
                return False
        return True

    def _matches_condition(self, doc: Document, condition: QueryCondition) -> bool:
        value = doc.get_field(condition.field)
        op = condition.operator
        target = condition.value

        if op == Operator.EXISTS:
            exists = value is not None
            return exists == target

        if value is None:
            return op == Operator.NE or op == Operator.NIN

        if op == Operator.EQ:
            return value == target
        elif op == Operator.NE:
            return value != target
        elif op == Operator.GT:
            return value > target
        elif op == Operator.GTE:
            return value >= target
        elif op == Operator.LT:
            return value < target
        elif op == Operator.LTE:
            return value <= target
        elif op == Operator.IN:
            return value in target
        elif op == Operator.NIN:
            return value not in target

        return False

    def explain(self, collection: str, query_dict: Dict[str, Any]) -> Dict[str, Any]:
        plan = self.optimize_query(collection, query_dict)
        return {
            'plan': plan.explain(),
            'cost_estimate': self._estimate_cost(collection, self.parse_query(query_dict)).__dict__,
        }
