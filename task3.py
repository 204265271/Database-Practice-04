import sqlite3
import random
import itertools
import time

def create_test_groupby_table(conn):
    """步骤 1: 创建表 testGroupby(A, B, C, D)"""
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS testGroupby (
        A INTEGER NOT NULL,
        B INTEGER NOT NULL,
        C INTEGER NOT NULL,
        D INTEGER NOT NULL
    )
    ''')
    conn.commit()

def generate_test_groupby_data(conn):
    num_records = random.randrange(500000, 1000000)
    """步骤 2: 填充 50~100 万行数据"""
    cursor = conn.cursor()
    cursor.execute("DELETE FROM testGroupby")  # 清空表
    for _ in range(num_records):
        A = random.randint(1, 10)
        B = random.randint(1, 100)
        C = random.randint(1, 1000)
        D = random.randint(1, 10000)
        cursor.execute("INSERT INTO testGroupby (A, B, C, D) VALUES (?, ?, ?, ?)", (A, B, C, D))
    conn.commit()
    return num_records

def compute_groupby_cardinalities(conn):
    """步骤 3: 查询 15 个分组属性集的行数，并存入 attrSets 表"""
    cursor = conn.cursor()

    # 创建 attrSets 表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS attrSets (
        attrCode TEXT PRIMARY KEY,
        cardinality INTEGER NOT NULL
    )
    ''')
    conn.commit()

    # 生成 15 个分组属性集
    attributes = ['A', 'B', 'C', 'D']
    groupings = []
    for i in range(1, len(attributes) + 1):
        groupings.extend(itertools.combinations(attributes, i))
        
    # print("print the groupings:", groupings)

    # 计算每个分组属性集的行数
    res = []
    for grouping in groupings:
        attr_code = ''.join(grouping)
        query = f"SELECT COUNT(*) FROM (SELECT {', '.join(grouping)} FROM testGroupby GROUP BY {', '.join(grouping)})"
        cursor.execute(query)
        cardinality = cursor.fetchone()[0]
        res.append([grouping, cardinality])
        cursor.execute("INSERT OR REPLACE INTO attrSets (attrCode, cardinality) VALUES (?, ?)", (attr_code, cardinality))
    conn.commit()
    return res

def generate_workloads(conn, num_queries):
    """步骤 4: 随机生成工作负载集"""
    cursor = conn.cursor()

    # 从 attrSets 表中随机选择若干行作为工作负载
    cursor.execute("SELECT attrCode FROM attrSets")
    attr_codes = [row[0] for row in cursor.fetchall()]
    workloads = random.sample(attr_codes, num_queries)

    # 创建 workloads 表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS workloads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        attrCode TEXT NOT NULL
    )
    ''')
    conn.commit()

    # 插入工作负载
    cursor.executemany("INSERT INTO workloads (attrCode) VALUES (?)", [(code,) for code in workloads])
    conn.commit()
    return workloads

def optimize_materialization(conn, cache_size):
    """步骤 5: 优化物化分组查询结果以最小化 IO"""
    cursor = conn.cursor()

    # 获取工作负载
    cursor.execute("SELECT attrCode FROM workloads")
    workloads = [row[0] for row in cursor.fetchall()]

    # 获取属性集的包含关系和行数
    cursor.execute("SELECT attrCode, cardinality FROM attrSets")
    attr_sets = {row[0]: row[1] for row in cursor.fetchall()}

    # 计算包含关系
    def is_subset(subset, superset):
        return all(attr in superset for attr in subset)

    # 优化选择
    materialized = set()
    total_io = 0
    for workload in workloads:
        workload_set = set(workload)
        # 查找是否可以从已物化的结果中计算得来
        found = False
        for mat in materialized:
            if is_subset(workload_set, set(mat)):
                total_io += attr_sets[mat]
                found = True
                break
        # 如果不能从已物化的结果中计算得来，则直接计算并物化
        if not found:
            total_io += attr_sets[workload]
            materialized.add(workload)
            # 如果超出缓存大小，则移除最小的物化结果
            if len(materialized) > cache_size:
                smallest = min(materialized, key=lambda x: attr_sets[x])
                materialized.remove(smallest)

    return total_io, materialized

def run_experiment(db_file):
    """步骤 6: 运行实验并对比不同负载集和缓存大小的结果"""
    conn = sqlite3.connect(db_file)

    # 创建表并生成数据
    create_test_groupby_table(conn)
    print("Step 1: already created the test table")
    
    generate_num = generate_test_groupby_data(conn)
    print(f"Step 2: already generated and put {generate_num} groups of data into the table")
    
    res = compute_groupby_cardinalities(conn)
    print("Step 3: compute group by cardinality")
    for r in res: 
        print(f"  grouping = {r[0]}; cardinality = {r[1]}")

    # 生成工作负载
    print("Step 4: randomly generate 10 workloads:", generate_workloads(conn, 10))  # 随机生成 10 个分组查询

    # 对比不同缓存大小的结果
    for cache_size in [2, 4, 6]:
        print(f"\n[INFO] Running with cache size = {cache_size}")
        total_io, materialized = optimize_materialization(conn, cache_size)
        print(f"Total IO: {total_io}")
        print(f"Materialized sets: {materialized}")

    conn.close()
