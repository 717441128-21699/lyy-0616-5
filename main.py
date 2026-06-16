#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
示例程序：演示支持 ACID 的文档数据库
"""

import os
import sys
import shutil
import json

if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass

from docdb import DocDB, Config, IsolationLevel


def cleanup_data():
    data_dir = "./data"
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)


def demo_basic_operations():
    print("\n" + "=" * 60)
    print("1. 基础操作演示（CRUD）")
    print("=" * 60)

    config = Config(data_dir="./data/demo1")
    with DocDB(config) as db:
        users = db["users"]

        print("\n--- 插入文档 ---")
        user1 = users.insert_one({"name": "张三", "age": 25, "email": "zhangsan@example.com", "city": "北京"})
        user2 = users.insert_one({"name": "李四", "age": 30, "email": "lisi@example.com", "city": "上海"})
        user3 = users.insert_one({"name": "王五", "age": 35, "email": "wangwu@example.com", "city": "北京"})
        print(f"插入文档1: {json.dumps(user1.data, ensure_ascii=False)}")
        print(f"插入文档2: {json.dumps(user2.data, ensure_ascii=False)}")
        print(f"插入文档3: {json.dumps(user3.data, ensure_ascii=False)}")

        print("\n--- 按ID查询 ---")
        found = users.find_one(user1.doc_id)
        if found:
            print(f"找到文档: {json.dumps(found.data, ensure_ascii=False)}")

        print("\n--- 更新文档 ---")
        updated = users.update_one(user1.doc_id, {"age": 26, "email": "zhangsan_new@example.com"})
        if updated:
            print(f"更新后文档: {json.dumps(updated.data, ensure_ascii=False)}")

        print("\n--- 删除文档 ---")
        deleted = users.delete_one(user3.doc_id)
        print(f"删除成功: {deleted}")

        print(f"\n文档总数: {users.count()}")


def demo_secondary_index():
    print("\n" + "=" * 60)
    print("2. 二级索引演示")
    print("=" * 60)

    config = Config(data_dir="./data/demo2")
    with DocDB(config) as db:
        users = db["users"]

        for i in range(20):
            users.insert_one({
                "name": f"用户{i}",
                "age": 20 + i,
                "city": ["北京", "上海", "广州", "深圳"][i % 4],
                "score": 60 + i * 2
            })

        print("\n--- 创建索引 ---")
        age_index = users.create_index("age")
        city_index = users.create_index("city")
        email_index = users.create_index("email", unique=True)
        print(f"创建索引: age, city, email(唯一)")
        print(f"索引列表: {[idx.field for idx in users.list_indexes()]}")

        print("\n--- 等值查询（使用索引） ---")
        query = {"age": 25}
        results, plan = users.find_with_plan(query)
        print(f"查询条件: {query}")
        print(f"执行计划: {plan.explain()['query_type']}, 使用索引: {plan.explain()['use_index']}")
        print(f"查询结果数: {len(results)}")
        for doc in results[:3]:
            print(f"  {json.dumps(doc.data, ensure_ascii=False)}")

        print("\n--- 范围查询（使用索引） ---")
        query = {"age": {"$gte": 25, "$lte": 30}}
        results, plan = users.find_with_plan(query)
        print(f"查询条件: {query}")
        print(f"执行计划: {plan.explain()['query_type']}, 使用索引: {plan.explain()['use_index']}")
        print(f"查询结果数: {len(results)}")
        for doc in results[:5]:
            print(f"  {json.dumps(doc.data, ensure_ascii=False)}")

        print("\n--- 查询执行计划详解 ---")
        explain = users.explain({"age": 25})
        print(f"成本估算: 全表扫描={explain['cost_estimate']['full_scan_cost']:.2f}, "
              f"索引扫描={explain['cost_estimate']['index_scan_cost']:.2f}")
        print(f"索引选择性: {explain['cost_estimate']['selectivity']:.4f}")

        print("\n--- 唯一索引冲突演示 ---")
        users.insert_one({"email": "unique@example.com", "name": "测试"})
        try:
            users.insert_one({"email": "unique@example.com", "name": "测试2"})
            print("ERROR: 应该抛出唯一约束异常!")
        except Exception as e:
            print(f"唯一约束冲突 (预期行为): {type(e).__name__}: {e}")


def demo_transactions():
    print("\n" + "=" * 60)
    print("3. 多文档事务演示")
    print("=" * 60)

    config = Config(data_dir="./data/demo3")
    with DocDB(config) as db:
        accounts = db["accounts"]

        accounts.insert_one({"account_id": "A001", "balance": 1000, "name": "账户A"})
        accounts.insert_one({"account_id": "A002", "balance": 1000, "name": "账户B"})

        print("\n--- 初始状态 ---")
        for doc in accounts.find():
            print(f"  {doc.data['account_id']}: {doc.data['balance']} 元")

        print("\n--- 转账事务 (A001 -> A002, 500元) ---")
        try:
            with db.transaction(IsolationLevel.REPEATABLE_READ) as txn:
                doc_a = accounts.find({"account_id": "A001"}, txn=txn)[0]
                doc_b = accounts.find({"account_id": "A002"}, txn=txn)[0]

                print(f"  读取: A001={doc_a.data['balance']}, A002={doc_b.data['balance']}")

                if doc_a.data["balance"] >= 500:
                    accounts.update_one(doc_a.doc_id, {"balance": doc_a.data["balance"] - 500}, txn=txn)
                    accounts.update_one(doc_b.doc_id, {"balance": doc_b.data["balance"] + 500}, txn=txn)
                    db.commit(txn)
                    print("  事务提交成功!")
                else:
                    db.abort(txn)
                    print("  余额不足, 事务回滚")
        except Exception as e:
            print(f"  事务失败: {e}")

        print("\n--- 转账后状态 ---")
        for doc in accounts.find():
            print(f"  {doc.data['account_id']}: {doc.data['balance']} 元")

        print("\n--- 事务回滚演示 ---")
        try:
            with db.transaction(IsolationLevel.REPEATABLE_READ) as txn:
                doc = accounts.find({"account_id": "A001"}, txn=txn)[0]
                accounts.update_one(doc.doc_id, {"balance": 999999}, txn=txn)
                print("  修改了余额但未提交...")
                raise Exception("模拟异常")
        except Exception as e:
            print(f"  异常触发回滚: {e}")

        print("\n--- 回滚后状态（余额应该恢复） ---")
        for doc in accounts.find():
            print(f"  {doc.data['account_id']}: {doc.data['balance']} 元")


def demo_index_maintenance():
    print("\n" + "=" * 60)
    print("4. 索引同步维护演示")
    print("=" * 60)

    config = Config(data_dir="./data/demo4")
    with DocDB(config) as db:
        users = db["users"]
        users.create_index("age")
        users.create_index("city")

        print("\n--- 插入文档时自动维护索引 ---")
        doc = users.insert_one({"name": "测试", "age": 25, "city": "杭州"})
        print(f"插入: age=25, city=杭州")

        results = users.find({"age": 25})
        print(f"按age=25查询: 找到 {len(results)} 条")

        print("\n--- 更新索引字段时同步更新索引 ---")
        users.update_one(doc.doc_id, {"age": 30, "city": "南京"})
        print(f"更新为: age=30, city=南京")

        results_old = users.find({"age": 25})
        results_new = users.find({"age": 30})
        results_city = users.find({"city": "南京"})
        print(f"按旧age=25查询: 找到 {len(results_old)} 条 (应该为0)")
        print(f"按新age=30查询: 找到 {len(results_new)} 条 (应该为1)")
        print(f"按新city=南京查询: 找到 {len(results_city)} 条 (应该为1)")

        print("\n--- 删除文档时同步删除索引 ---")
        users.delete_one(doc.doc_id)
        results_after = users.find({"age": 30})
        print(f"删除后按age=30查询: 找到 {len(results_after)} 条 (应该为0)")


def demo_query_optimizer():
    print("\n" + "=" * 60)
    print("5. 查询优化器演示（索引选择 vs 全扫描）")
    print("=" * 60)

    config = Config(data_dir="./data/demo5")
    with DocDB(config) as db:
        products = db["products"]

        for i in range(1000):
            category = ["电子产品", "服装", "食品", "图书"][i % 4]
            products.insert_one({
                "name": f"商品{i}",
                "price": 10 + i,
                "category": category,
                "stock": i % 100,
                "rating": 3.0 + (i % 20) * 0.1
            })

        products.create_index("price")
        products.create_index("category")
        products.create_index("stock")

        print("\n--- 高选择性查询（使用索引） ---")
        query = {"price": 500}
        explain = products.explain(query)
        print(f"查询: price = 500")
        print(f"  选择性: {explain['cost_estimate']['selectivity']:.4f}")
        print(f"  全表扫描成本: {explain['cost_estimate']['full_scan_cost']:.2f}")
        print(f"  索引扫描成本: {explain['cost_estimate']['index_scan_cost']:.2f}")
        print(f"  决策: {'使用索引' if explain['plan']['use_index'] else '全表扫描'}")

        print("\n--- 低选择性查询（全表扫描） ---")
        query = {"category": "电子产品"}
        explain = products.explain(query)
        print(f"查询: category = '电子产品'")
        print(f"  选择性: {explain['cost_estimate']['selectivity']:.4f}")
        print(f"  全表扫描成本: {explain['cost_estimate']['full_scan_cost']:.2f}")
        print(f"  索引扫描成本: {explain['cost_estimate']['index_scan_cost']:.2f}")
        print(f"  决策: {'使用索引' if explain['plan']['use_index'] else '全表扫描'}")

        print("\n--- 范围查询 ---")
        query = {"price": {"$gte": 100, "$lte": 200}}
        results, plan = products.find_with_plan(query)
        print(f"查询: price BETWEEN 100 AND 200")
        print(f"  执行计划: {plan.query_type.name}")
        print(f"  使用索引: {plan.use_index}")
        print(f"  结果数: {len(results)}")


def demo_concurrency():
    print("\n" + "=" * 60)
    print("6. 并发控制与死锁避免演示")
    print("=" * 60)

    config = Config(data_dir="./data/demo6")
    with DocDB(config) as db:
        items = db["items"]
        item1 = items.insert_one({"name": "资源A", "value": 100})
        item2 = items.insert_one({"name": "资源B", "value": 200})

        print("\n--- 锁信息查看 ---")
        with db.transaction(IsolationLevel.REPEATABLE_READ) as txn:
            doc = items.find_one(item1.doc_id, txn=txn)
            print(f"事务 {txn.txn_id} 读取了资源A")

            lock_info = db.get_lock_info()
            print(f"等待图: {lock_info['wait_for_graph']}")
            for resource, info in lock_info['locks'].items():
                print(f"  {resource}: holders={info['holders']}")

            db.commit(txn)

        print("\n--- Wait-Die 死锁避免策略 ---")
        print("策略说明:")
        print("  - 旧事务（ID较小）等待新事务释放锁")
        print("  - 新事务请求旧事务持有的锁时，直接回滚（死亡）")
        print("  - 这样可以避免循环等待，从根源上防止死锁")

        print("\n--- 死锁检测 ---")
        print("系统同时维护等待图，定期进行DFS环检测:")
        print("  - 如果检测到环，选择最年轻（代价最小）的事务作为牺牲品回滚")
        print("  - 保证系统不会陷入死锁状态")


def demo_crash_recovery():
    print("\n" + "=" * 60)
    print("7. 崩溃恢复机制演示")
    print("=" * 60)

    print("\n--- WAL 持久化机制 ---")
    print("所有修改操作在写入数据文件前，先写入 WAL:")
    print("  1. 预写原则: 先写日志，再写数据")
    print("  2. 检查点: 定期标记数据已落盘的位置")
    print("  3. 日志包含数据和索引的所有修改")

    print("\n--- 崩溃恢复流程 (ARIES 风格) ---")
    print("1. 分析阶段: 扫描 WAL，识别已提交和未完成的事务")
    print("2. 重做阶段: 从检查点开始，重做所有已提交事务的操作")
    print("3. 回滚阶段: 撤销所有未提交事务的操作")
    print("4. 一致性检查: 验证索引与数据的一致性")
    print("5. 修复不一致: 重建索引或补充缺失的索引条目")

    print("\n--- 索引与数据一致性保证 ---")
    print("问题: 如果崩溃发生在数据已写入但索引未更新时，会出现悬挂索引")
    print("解决方案:")
    print("  1. WAL 同时记录数据和索引的修改")
    print("  2. 恢复时，数据和索引的修改一起重做/回滚")
    print("  3. 最后进行全量一致性检查:")
    print("     - 遍历所有索引条目，检查指向的文档是否存在")
    print("     - 遍历所有文档，检查应该存在的索引是否存在")
    print("  4. 发现不一致时自动修复（重建索引或补充条目）")

    config = Config()
    with DocDB(config) as db:
        recovery_stats = db.get_recovery_stats()
        if recovery_stats:
            print(f"\n上次恢复统计:")
            print(f"  恢复时间: {recovery_stats['last_recovery']['recovery_time_ms']:.2f} ms")
            print(f"  重做操作数: {recovery_stats['last_recovery']['redo_count']}")
            print(f"  回滚操作数: {recovery_stats['last_recovery']['undo_count']}")
            print(f"  修复的不一致索引: {recovery_stats['last_recovery']['fixed_indexes']}")


def main():
    print("""
╔══════════════════════════════════════════════════════════════╗
║          支持 ACID 的文档数据库 - 功能演示                   ║
╚══════════════════════════════════════════════════════════════╝

项目模块结构:
  docdb/
    ├── common/          # 公共模块（文档模型、异常、配置）
    ├── storage/         # 文档存储引擎
    ├── index/           # B树二级索引
    ├── wal/             # 预写日志
    ├── concurrency/     # 锁管理器与死锁避免
    ├── transaction/     # 事务管理器（MVCC）
    ├── query/           # 查询优化器
    ├── recovery/        # 崩溃恢复
    └── api/             # 数据库API入口

核心特性:
  * JSON文档存储        * B树二级索引        * WAL持久化
  * 多文档事务          * MVCC隔离           * 死锁检测/避免
  * 查询优化器          * 崩溃恢复           * 索引一致性保证
""")

    try:
        cleanup_data()

        demo_basic_operations()
        demo_secondary_index()
        demo_transactions()
        demo_index_maintenance()
        demo_query_optimizer()
        demo_concurrency()
        demo_crash_recovery()

        print("\n" + "=" * 60)
        print("演示完成!")
        print("=" * 60)

    except Exception as e:
        print(f"\n错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cleanup_data()


if __name__ == "__main__":
    main()
