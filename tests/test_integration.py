#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
集成测试：验证文档数据库的所有核心功能
"""

import os
import sys
import shutil
import unittest
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from docdb import DocDB, Config, IsolationLevel
from docdb.common import DocumentNotFoundError, UniqueConstraintViolationError


class TestDocDBIntegration(unittest.TestCase):
    def setUp(self):
        self.data_dir = "./test_data"
        self._cleanup()
        self.config = Config(data_dir=self.data_dir)
        self.db = DocDB(self.config)

    def tearDown(self):
        self.db.close()
        self._cleanup()

    def _cleanup(self):
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)

    def test_basic_crud(self):
        """测试基础CRUD操作"""
        users = self.db["users"]

        doc1 = users.insert_one({"name": "测试用户", "age": 25, "email": "test@example.com"})
        self.assertIsNotNone(doc1.doc_id)
        self.assertEqual(doc1.data["name"], "测试用户")

        found = users.find_one(doc1.doc_id)
        self.assertIsNotNone(found)
        self.assertEqual(found.data["name"], "测试用户")

        updated = users.update_one(doc1.doc_id, {"age": 26, "city": "北京"})
        self.assertIsNotNone(updated)
        self.assertEqual(updated.data["age"], 26)
        self.assertEqual(updated.data["city"], "北京")

        found_updated = users.find_one(doc1.doc_id)
        self.assertEqual(found_updated.data["age"], 26)

        result = users.delete_one(doc1.doc_id)
        self.assertTrue(result)

        found_deleted = users.find_one(doc1.doc_id)
        self.assertIsNone(found_deleted)

    def test_secondary_index(self):
        """测试二级索引"""
        users = self.db["users"]

        for i in range(50):
            users.insert_one({
                "name": f"用户{i}",
                "age": 20 + i,
                "city": ["北京", "上海", "广州", "深圳"][i % 4],
            })

        users.create_index("age")
        users.create_index("city")

        results = users.find({"age": 25})
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].data["age"], 25)

        results = users.find({"city": "北京"})
        self.assertEqual(len(results), 13)

        results, plan = users.find_with_plan({"age": {"$gte": 30, "$lte": 40}})
        self.assertIsNotNone(plan.use_index)
        self.assertEqual(len(results), 11)

    def test_unique_index(self):
        """测试唯一索引"""
        users = self.db["users"]
        users.create_index("email", unique=True)

        users.insert_one({"email": "unique@test.com", "name": "用户1"})

        with self.assertRaises(UniqueConstraintViolationError):
            users.insert_one({"email": "unique@test.com", "name": "用户2"})

    def test_transaction_commit(self):
        """测试事务提交"""
        accounts = self.db["accounts"]
        accounts.insert_one({"account_id": "A1", "balance": 1000})
        accounts.insert_one({"account_id": "A2", "balance": 1000})

        with self.db.transaction(IsolationLevel.REPEATABLE_READ) as txn:
            doc_a = accounts.find({"account_id": "A1"})[0]
            doc_b = accounts.find({"account_id": "A2"})[0]

            self.db._transaction_manager.update_document(
                txn, "accounts", doc_a.doc_id, {"balance": 500}
            )
            self.db._transaction_manager.update_document(
                txn, "accounts", doc_b.doc_id, {"balance": 1500}
            )
            self.db.commit(txn)

        doc_a = accounts.find({"account_id": "A1"})[0]
        doc_b = accounts.find({"account_id": "A2"})[0]
        self.assertEqual(doc_a.data["balance"], 500)
        self.assertEqual(doc_b.data["balance"], 1500)

    def test_transaction_rollback(self):
        """测试事务回滚"""
        accounts = self.db["accounts"]
        doc = accounts.insert_one({"account_id": "A1", "balance": 1000})

        try:
            with self.db.transaction(IsolationLevel.REPEATABLE_READ) as txn:
                self.db._transaction_manager.update_document(
                    txn, "accounts", doc.doc_id, {"balance": 999999}
                )
                raise Exception("模拟异常")
        except Exception:
            pass

        updated = accounts.find_one(doc.doc_id)
        self.assertEqual(updated.data["balance"], 1000)

    def test_index_maintenance_on_update(self):
        """测试更新时索引同步维护"""
        users = self.db["users"]
        users.create_index("age")
        users.create_index("city")

        doc = users.insert_one({"name": "测试", "age": 25, "city": "杭州"})

        results = users.find({"age": 25})
        self.assertEqual(len(results), 1)

        users.update_one(doc.doc_id, {"age": 30, "city": "南京"})

        results_old_age = users.find({"age": 25})
        results_new_age = users.find({"age": 30})
        results_new_city = users.find({"city": "南京"})

        self.assertEqual(len(results_old_age), 0)
        self.assertEqual(len(results_new_age), 1)
        self.assertEqual(len(results_new_city), 1)

    def test_index_maintenance_on_delete(self):
        """测试删除时索引同步维护"""
        users = self.db["users"]
        users.create_index("age")

        doc = users.insert_one({"name": "测试", "age": 25})
        self.assertEqual(len(users.find({"age": 25})), 1)

        users.delete_one(doc.doc_id)
        self.assertEqual(len(users.find({"age": 25})), 0)

    def test_query_optimizer_decision(self):
        """测试查询优化器决策"""
        products = self.db["products"]

        for i in range(500):
            products.insert_one({
                "name": f"商品{i}",
                "price": 10 + i,
                "category": ["A", "B", "C", "D"][i % 4],
            })

        products.create_index("price")
        products.create_index("category")

        explain_high = products.explain({"price": 250})
        self.assertIsNotNone(explain_high["plan"]["use_index"])
        self.assertGreater(
            explain_high["cost_estimate"]["full_scan_cost"],
            explain_high["cost_estimate"]["index_scan_cost"]
        )

        explain_low = products.explain({"category": "A"})
        selectivity = explain_low["cost_estimate"]["selectivity"]
        self.assertLess(selectivity, 0.5)

    def test_isolation_levels(self):
        """测试不同隔离级别"""
        users = self.db["users"]
        doc = users.insert_one({"name": "测试", "age": 25})

        with self.db.transaction(IsolationLevel.READ_COMMITTED) as txn:
            d = self.db._transaction_manager.read_document(txn, "users", doc.doc_id)
            self.assertEqual(d.data["age"], 25)
            self.db.commit(txn)

        with self.db.transaction(IsolationLevel.REPEATABLE_READ) as txn:
            d1 = self.db._transaction_manager.read_document(txn, "users", doc.doc_id)
            self.db._transaction_manager.update_document(txn, "users", doc.doc_id, {"age": 30})
            d2 = self.db._transaction_manager.read_document(txn, "users", doc.doc_id)
            self.assertEqual(d2.data["age"], 30)
            self.db.commit(txn)

        with self.db.transaction(IsolationLevel.REPEATABLE_READ) as txn:
            d3 = self.db._transaction_manager.read_document(txn, "users", doc.doc_id)
            self.assertEqual(d3.data["age"], 30)
            self.db.commit(txn)

    def test_serializable_isolation(self):
        """测试 SERIALIZABLE 隔离级别"""
        users = self.db["users"]
        doc = users.insert_one({"name": "串行化测试", "age": 25})

        with self.db.transaction(IsolationLevel.SERIALIZABLE) as txn:
            d = self.db._transaction_manager.read_document(txn, "users", doc.doc_id)
            self.assertEqual(d.data["age"], 25)
            self.db.commit(txn)

        with self.db.transaction(IsolationLevel.SERIALIZABLE) as txn:
            d1 = self.db._transaction_manager.read_document(txn, "users", doc.doc_id)
            self.assertEqual(d1.data["age"], 25)
            d2 = self.db._transaction_manager.read_document(txn, "users", doc.doc_id)
            self.assertEqual(d2.data["age"], 25)
            self.db.commit(txn)

        with self.db.transaction(IsolationLevel.SERIALIZABLE) as txn:
            d = self.db._transaction_manager.read_document(txn, "users", doc.doc_id)
            self.db._transaction_manager.update_document(txn, "users", doc.doc_id, {"age": 30})
            self.db.commit(txn)

        doc2 = users.find_one(doc.doc_id)
        self.assertEqual(doc2.data["age"], 30)

    def test_concurrent_transactions(self):
        """测试并发事务"""
        from docdb.common import TransactionAbortedError

        counter = self.db["counter"]
        counter.insert_one({"name": "count", "value": 0})
        success_count = [0]
        success_lock = threading.Lock()
        num_threads = 5
        iterations = 20

        def increment():
            for _ in range(iterations):
                for attempt in range(100):
                    try:
                        with self.db.transaction(IsolationLevel.SERIALIZABLE) as txn:
                            doc = counter.find({"name": "count"})[0]
                            new_val = doc.data["value"] + 1
                            self.db._transaction_manager.update_document(
                                txn, "counter", doc.doc_id, {"value": new_val}
                            )
                            self.db.commit(txn)
                            with success_lock:
                                success_count[0] += 1
                        break
                    except TransactionAbortedError:
                        time.sleep(0.001 * attempt)
                        continue

        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=increment)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        doc = counter.find({"name": "count"})[0]
        self.assertEqual(doc.data["value"], success_count[0])
        self.assertEqual(success_count[0], num_threads * iterations)

    def test_multiple_collections(self):
        """测试多个集合"""
        users = self.db["users"]
        orders = self.db["orders"]

        user = users.insert_one({"name": "用户", "email": "user@test.com"})
        order = orders.insert_one({"user_id": user.doc_id, "amount": 99.99})

        self.assertEqual(len(self.db.list_collections()), 2)

        found_user = users.find_one(user.doc_id)
        found_order = orders.find_one(order.doc_id)
        self.assertIsNotNone(found_user)
        self.assertIsNotNone(found_order)

    def test_drop_collection(self):
        """测试删除集合"""
        users = self.db["users"]
        users.insert_one({"name": "测试"})

        self.assertIn("users", self.db.list_collections())

        self.db.drop_collection("users")
        self.assertNotIn("users", self.db.list_collections())

    def test_document_not_found(self):
        """测试文档不存在异常"""
        users = self.db["users"]

        with self.assertRaises(DocumentNotFoundError):
            with self.db.transaction() as txn:
                self.db._transaction_manager.read_document(
                    txn, "users", "non-existent-id"
                )

    def test_count_documents(self):
        """测试文档计数"""
        users = self.db["users"]

        for i in range(10):
            users.insert_one({"name": f"用户{i}"})

        self.assertEqual(users.count(), 10)

        users.delete_one(users.find()[0].doc_id)
        self.assertEqual(users.count(), 9)

    def test_find_all(self):
        """测试查询所有文档"""
        users = self.db["users"]

        for i in range(5):
            users.insert_one({"name": f"用户{i}", "age": 20 + i})

        all_docs = users.find()
        self.assertEqual(len(all_docs), 5)

        filtered = users.find({"age": {"$gte": 22}})
        self.assertEqual(len(filtered), 3)

    def test_explain_query(self):
        """测试查询解释"""
        users = self.db["users"]
        users.create_index("age")

        for i in range(10):
            users.insert_one({"name": f"用户{i}", "age": 20 + i})

        explain = users.explain({"age": 25})
        self.assertIn("plan", explain)
        self.assertIn("cost_estimate", explain)
        self.assertIsNotNone(explain["plan"]["use_index"])

    def test_list_indexes(self):
        """测试列出索引"""
        users = self.db["users"]
        users.create_index("age")
        users.create_index("email", unique=True)

        indexes = users.list_indexes()
        self.assertEqual(len(indexes), 2)

        fields = [idx.field for idx in indexes]
        self.assertIn("age", fields)
        self.assertIn("email", fields)

    def test_drop_index(self):
        """测试删除索引"""
        users = self.db["users"]
        users.create_index("age")
        users.create_index("city")

        self.assertEqual(len(users.list_indexes()), 2)

        users.drop_index("age")
        self.assertEqual(len(users.list_indexes()), 1)

    def test_checkpoint(self):
        """测试检查点"""
        users = self.db["users"]
        users.insert_one({"name": "测试"})

        lsn = self.db.checkpoint()
        self.assertGreater(lsn, 0)

    def test_lock_info(self):
        """测试锁信息"""
        users = self.db["users"]
        doc = users.insert_one({"name": "测试"})

        with self.db.transaction() as txn:
            self.db._transaction_manager.read_document(txn, "users", doc.doc_id)

            lock_info = self.db.get_lock_info()
            self.assertIsNotNone(lock_info)
            self.assertIn("wait_for_graph", lock_info)
            self.assertIn("locks", lock_info)

            self.db.commit(txn)


if __name__ == "__main__":
    unittest.main(verbosity=2)
