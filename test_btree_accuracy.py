#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试B树索引在大数据量下的准确性
"""

import os
import sys
import shutil

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from docdb import DocDB, Config

def test_btree_accuracy():
    """测试B树在数据量超过单个节点容量后的查询准确性"""
    data_dir = "./test_btree_data"
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    
    config = Config(data_dir=data_dir)
    print(f"B树阶数: {config.btree_order}")
    print(f"单个节点最大键数: {config.btree_order - 1}")
    
    with DocDB(config) as db:
        items = db["items"]
        items.create_index("value")
        
        # 插入超过单个节点容量的数据（阶数100，节点最大99个键）
        # 插入200条连续数值，确保触发多次节点分裂
        num_docs = 200
        print(f"\n插入 {num_docs} 条文档，value 从 0 到 {num_docs - 1}...")
        
        for i in range(num_docs):
            items.insert_one({
                "name": f"item_{i}",
                "value": i,
                "extra": f"data_{i}"
            })
        
        print(f"文档总数: {items.count()}")
        
        # 逐个等值查询验证
        print("\n逐个等值查询验证...")
        errors = []
        
        for i in range(num_docs):
            results = items.find({"value": i})
            if len(results) != 1:
                errors.append(f"value={i}: 期望1条，实际{len(results)}条")
            elif results[0].data["value"] != i:
                errors.append(f"value={i}: 查询结果错误，得到value={results[0].data['value']}")
        
        if errors:
            print(f"\n❌ 发现 {len(errors)} 个错误:")
            for err in errors[:20]:
                print(f"  {err}")
            if len(errors) > 20:
                print(f"  ... 还有 {len(errors) - 20} 个错误")
        else:
            print("✅ 所有等值查询都正确！")
        
        # 范围查询验证
        print("\n范围查询验证...")
        range_errors = []
        
        test_ranges = [
            (0, 50),
            (50, 100),
            (100, 150),
            (150, 200),
            (25, 75),
            (0, 199),
        ]
        
        for start, end in test_ranges:
            query = {"value": {"$gte": start, "$lte": end - 1}}
            results = items.find(query)
            expected_count = end - start
            if len(results) != expected_count:
                range_errors.append(
                    f"范围 [{start}, {end-1}]: 期望{expected_count}条，实际{len(results)}条"
                )
            else:
                values = sorted([r.data["value"] for r in results])
                expected_values = list(range(start, end))
                if values != expected_values:
                    range_errors.append(
                        f"范围 [{start}, {end-1}]: 结果值不正确"
                    )
        
        if range_errors:
            print(f"\n❌ 范围查询发现 {len(range_errors)} 个错误:")
            for err in range_errors:
                print(f"  {err}")
        else:
            print("✅ 所有范围查询都正确！")
    
    # 清理
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    
    print("\n测试完成！")
    return len(errors) == 0 and len(range_errors) == 0

if __name__ == "__main__":
    success = test_btree_accuracy()
    sys.exit(0 if success else 1)
