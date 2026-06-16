#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试用户新需求的脚本
"""
import os
import sys
import shutil
import threading
import time

os.environ['PYTHONIOENCODING'] = 'utf-8'

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from docdb import DocDB, Config, IsolationLevel
from docdb.common import TransactionAbortedError


def test_repeatable_read_with_real_transactions():
    """测试可重复读：两个真实事务交叉验证"""
    print("\n" + "="*60)
    print("测试：可重复读隔离级别（真实事务交叉验证）")
    print("="*60)
    
    data_dir = "./test_rr_real"
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    
    config = Config(data_dir=data_dir)
    db = DocDB(config)
    users = db["users"]
    
    # 初始数据：age=40
    doc = users.insert_one({"name": "测试用户", "age": 40, "balance": 1000})
    doc_id = doc.doc_id
    print(f"初始文档: age=40, doc_id={doc_id}")
    
    # 用于同步的事件
    txn1_read_done = threading.Event()
    txn2_commit_done = threading.Event()
    txn1_result = {"first_read": None, "second_read": None, "error": None}
    txn2_result = {"updated": False, "error": None}
    final_result = {"value": None}
    
    def transaction1():
        """事务1：先读，等事务2修改提交后再读"""
        try:
            with db.transaction(IsolationLevel.REPEATABLE_READ) as txn:
                print(f"\n[事务1] 开始，ID={txn.txn_id}")
                
                # 第一次读取
                d1 = db._transaction_manager.read_document(txn, "users", doc_id)
                txn1_result["first_read"] = d1.data["age"]
                print(f"[事务1] 第1次读取: age={d1.data['age']}")
                
                # 通知事务2可以开始修改
                txn1_read_done.set()
                
                # 等待事务2提交完成
                print("[事务1] 等待事务2提交...")
                txn2_commit_done.wait(timeout=10)
                
                # 第二次读取 - 应该还是40
                d2 = db._transaction_manager.read_document(txn, "users", doc_id)
                txn1_result["second_read"] = d2.data["age"]
                print(f"[事务1] 第2次读取: age={d2.data['age']}")
                
                # 检查 read_set
                key = txn.get_write_key("users", doc_id)
                if key in txn.read_set:
                    print(f"[事务1] read_set 中有缓存: age={txn.read_set[key].data['age']}")
                
                db.commit(txn)
                print(f"[事务1] 提交成功")
        except Exception as e:
            txn1_result["error"] = str(e)
            print(f"[事务1] 错误: {e}")
    
    def transaction2():
        """事务2：等事务1第一次读后，修改并提交"""
        try:
            # 等待事务1第一次读取完成
            txn1_read_done.wait(timeout=10)
            
            with db.transaction(IsolationLevel.READ_COMMITTED) as txn:
                print(f"\n[事务2] 开始，ID={txn.txn_id}")
                
                # 修改 age 为 999
                d = db._transaction_manager.update_document(txn, "users", doc_id, {"age": 999})
                print(f"[事务2] 修改文档: age={d.data['age']}")
                
                db.commit(txn)
                txn2_result["updated"] = True
                print(f"[事务2] 提交成功，age=999")
            
            # 通知事务1可以继续第二次读取
            txn2_commit_done.set()
            
        except Exception as e:
            txn2_result["error"] = str(e)
            txn2_commit_done.set()  # 即使失败也通知，避免死等
            print(f"[事务2] 错误: {e}")
    
    # 启动两个线程
    t1 = threading.Thread(target=transaction1)
    t2 = threading.Thread(target=transaction2)
    
    t1.start()
    t2.start()
    
    t1.join(timeout=15)
    t2.join(timeout=15)
    
    # 最终验证：新事务读取应该看到999
    print("\n--- 最终验证 ---")
    try:
        with db.transaction(IsolationLevel.READ_COMMITTED) as txn:
            d = db._transaction_manager.read_document(txn, "users", doc_id)
            final_result["value"] = d.data["age"]
            print(f"[新事务] 读取: age={d.data['age']}")
            db.commit(txn)
    except Exception as e:
        print(f"[新事务] 错误: {e}")
    
    # 结果判断
    print("\n--- 结果判断 ---")
    all_passed = True
    
    if txn1_result["error"]:
        print(f"❌ 事务1出错: {txn1_result['error']}")
        all_passed = False
    if txn2_result["error"]:
        print(f"❌ 事务2出错: {txn2_result['error']}")
        all_passed = False
    
    if txn1_result["first_read"] != 40:
        print(f"❌ 事务1第一次读取错误: 期望40, 实际{txn1_result['first_read']}")
        all_passed = False
    else:
        print("✅ 事务1第一次读取正确: age=40")
    
    if not txn2_result["updated"]:
        print("❌ 事务2未成功修改")
        all_passed = False
    else:
        print("✅ 事务2修改并提交成功: age=999")
    
    if txn1_result["second_read"] != 40:
        print(f"❌ 事务1第二次读取错误: 期望40, 实际{txn1_result['second_read']}")
        all_passed = False
    else:
        print("✅ 事务1第二次读取正确: 仍为40（可重复读）")
    
    if final_result["value"] != 999:
        print(f"❌ 最终验证错误: 期望999, 实际{final_result['value']}")
        all_passed = False
    else:
        print("✅ 最终验证正确: 事务1结束后新事务看到999")
    
    db.close()
    
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    
    return all_passed


def test_serializable():
    """测试 SERIALIZABLE 隔离级别"""
    print("\n" + "="*60)
    print("测试：SERIALIZABLE 隔离级别")
    print("="*60)
    
    data_dir = "./test_serializable"
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    
    config = Config(data_dir=data_dir)
    db = DocDB(config)
    users = db["users"]
    
    doc = users.insert_one({"name": "测试用户", "age": 25})
    doc_id = doc.doc_id
    print(f"初始文档: age=25")
    
    all_passed = True
    
    # 测试1：读后提交
    print("\n测试1：SERIALIZABLE 事务读后提交")
    try:
        with db.transaction(IsolationLevel.SERIALIZABLE) as txn:
            print(f"  事务ID: {txn.txn_id}")
            d = db._transaction_manager.read_document(txn, "users", doc_id)
            print(f"  读取: age={d.data['age']}")
            db.commit(txn)
        print("  ✅ SERIALIZABLE 读后提交成功")
    except Exception as e:
        print(f"  ❌ SERIALIZABLE 读后提交失败: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    # 测试2：读后提交，再次读取
    print("\n测试2：SERIALIZABLE 事务读后再次读取")
    try:
        with db.transaction(IsolationLevel.SERIALIZABLE) as txn:
            print(f"  事务ID: {txn.txn_id}")
            d1 = db._transaction_manager.read_document(txn, "users", doc_id)
            print(f"  第1次读取: age={d1.data['age']}")
            d2 = db._transaction_manager.read_document(txn, "users", doc_id)
            print(f"  第2次读取: age={d2.data['age']}")
            db.commit(txn)
        print("  ✅ SERIALIZABLE 多次读取后提交成功")
    except Exception as e:
        print(f"  ❌ SERIALIZABLE 多次读取后提交失败: {e}")
        all_passed = False
    
    # 测试3：SERIALIZABLE 并发验证（写-读冲突）
    print("\n测试3：SERIALIZABLE 并发验证")
    doc2 = users.insert_one({"name": "并发测试", "age": 100})
    doc2_id = doc2.doc_id
    
    txn1_read = threading.Event()
    txn1_continue = threading.Event()
    ser_result = {"txn1_ok": False, "txn2_ok": False}
    
    def serial_txn1():
        try:
            with db.transaction(IsolationLevel.SERIALIZABLE) as txn:
                print(f"  [事务1] 开始，ID={txn.txn_id}")
                d = db._transaction_manager.read_document(txn, "users", doc2_id)
                print(f"  [事务1] 读取: age={d.data['age']}")
                txn1_read.set()
                time.sleep(0.5)  # 等待事务2尝试修改
                db.commit(txn)
                ser_result["txn1_ok"] = True
                print(f"  [事务1] 提交成功")
        except TransactionAbortedError as e:
            print(f"  [事务1] 被回滚（预期行为）: {e}")
    
    def serial_txn2():
        try:
            txn1_read.wait(timeout=5)
            with db.transaction(IsolationLevel.READ_COMMITTED) as txn:
                print(f"  [事务2] 开始，ID={txn.txn_id}")
                d = db._transaction_manager.update_document(txn, "users", doc2_id, {"age": 200})
                print(f"  [事务2] 修改: age={d.data['age']}")
                db.commit(txn)
                ser_result["txn2_ok"] = True
                print(f"  [事务2] 提交成功")
        except Exception as e:
            print(f"  [事务2] 错误: {e}")
    
    t1 = threading.Thread(target=serial_txn1)
    t2 = threading.Thread(target=serial_txn2)
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)
    
    if ser_result["txn2_ok"]:
        print("  ✅ SERIALIZABLE 并发测试：事务2提交成功")
    else:
        print("  ⚠️  SERIALIZABLE 并发测试：事务2被回滚（可能因锁冲突）")
    
    if ser_result["txn1_ok"]:
        print("  ✅ SERIALIZABLE 并发测试：事务1提交成功")
    
    db.close()
    
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    
    return all_passed


def test_concurrent_counter():
    """测试并发自增：确保最终计数与成功提交次数一致"""
    print("\n" + "="*60)
    print("测试：并发自增计数（确保无丢失更新）")
    print("="*60)
    
    data_dir = "./test_concurrent_counter"
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    
    config = Config(data_dir=data_dir)
    db = DocDB(config)
    counter = db["counter"]
    
    counter.insert_one({"name": "count", "value": 0})
    
    success_count = [0]
    success_lock = threading.Lock()
    num_threads = 5
    iterations = 20
    
    def increment():
        for _ in range(iterations):
            for attempt in range(100):
                try:
                    with db.transaction(IsolationLevel.SERIALIZABLE) as txn:
                        # 使用 SERIALIZABLE + 先读后写，避免丢失更新
                        doc = counter.find({"name": "count"})[0]
                        new_val = doc.data["value"] + 1
                        db._transaction_manager.update_document(
                            txn, "counter", doc.doc_id, {"value": new_val}
                        )
                        db.commit(txn)
                        with success_lock:
                            success_count[0] += 1
                    break
                except TransactionAbortedError:
                    time.sleep(0.001 * attempt)
                    continue
    
    threads = []
    start_time = time.time()
    for i in range(num_threads):
        t = threading.Thread(target=increment)
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    elapsed = time.time() - start_time
    doc = counter.find({"name": "count"})[0]
    final_value = doc.data["value"]
    
    print(f"\n线程数: {num_threads}, 每线程迭代: {iterations}")
    print(f"总尝试次数: {num_threads * iterations}")
    print(f"成功提交次数: {success_count[0]}")
    print(f"最终计数值: {final_value}")
    print(f"耗时: {elapsed:.2f}秒")
    
    all_passed = True
    if final_value == success_count[0]:
        print("✅ 最终计数值与成功提交次数一致，无丢失更新")
    else:
        print(f"❌ 最终计数值({final_value})与成功提交次数({success_count[0]})不一致，存在丢失更新")
        all_passed = False
    
    if success_count[0] == num_threads * iterations:
        print(f"✅ 全部 {num_threads * iterations} 次操作成功")
    else:
        print(f"⚠️  部分操作被回滚: {num_threads * iterations - success_count[0]} 次")
    
    db.close()
    
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    
    return all_passed


if __name__ == "__main__":
    print("╔" + "═"*58 + "╗")
    print("║" + " "*10 + "用户新需求验证测试" + " "*30 + "║")
    print("╚" + "═"*58 + "╝")
    
    results = []
    
    # 测试1：可重复读真实事务验证
    results.append(("可重复读真实事务", test_repeatable_read_with_real_transactions()))
    
    # 测试2：SERIALIZABLE
    results.append(("SERIALIZABLE 隔离级别", test_serializable()))
    
    # 测试3：并发自增
    results.append(("并发自增计数", test_concurrent_counter()))
    
    # 汇总
    print("\n" + "="*60)
    print("测试结果汇总")
    print("="*60)
    
    all_passed = True
    for name, passed in results:
        status = "✅ 通过" if passed else "❌ 失败"
        print(f"  {name}: {status}")
        if not passed:
            all_passed = False
    
    if all_passed:
        print("\n🎉 所有测试全部通过！")
    else:
        print("\n⚠️  部分测试失败，请检查")
        sys.exit(1)
