import sqlite3
import time

def create_and_populate_table(db_path):
    # 创建数据库连接
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 创建 testIndex 表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS testIndex (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        A INTEGER NOT NULL,
        B INTEGER NOT NULL,
        C TEXT NOT NULL
    )
    ''')
    conn.commit()

    # 填充数据
    print("[INFO] Populating data...")
    cursor.execute("DELETE FROM testIndex")  # 清空表
    for i in range(1, 500001):  # 填充 50 万条数据
        cursor.execute("INSERT INTO testIndex (A, B, C) VALUES (?, ?, ?)", (i % 100, i % 50, f"String_{i}"))
    conn.commit()
    print("[INFO] Data population completed.")

    cursor.close()
    conn.close()

def experiment_1(db_path):
    # 针对 A 列的分组和自连接操作，观察索引前后的性能差异
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("\n[Experiment 1] A 列分组和自连接操作")
    # 无索引
    cursor.execute("DROP INDEX IF EXISTS idx_A")
    conn.commit()
    start_time = time.time()
    cursor.execute("SELECT A, COUNT(*) FROM testIndex GROUP BY A")
    cursor.fetchall()
    interval_1 = time.time() - start_time
    print(f"Without index on A: {interval_1:.6f} seconds")

    # 创建索引
    cursor.execute("CREATE INDEX idx_A ON testIndex(A)")
    conn.commit()
    start_time = time.time()
    cursor.execute("SELECT A, COUNT(*) FROM testIndex GROUP BY A")
    cursor.fetchall()
    interval_2 = time.time() - start_time
    print(f"With index on A: {interval_2:.6f} seconds")
    print(f"ratio = {interval_1 / interval_2:.6f}")

    cursor.close()
    conn.close()

def experiment_2(db_path):
    # 针对 select B where A 类型的查询，观察组合索引和单列索引的性能差异
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("\n[Experiment 2] select B where A 查询")
    # 单列索引
    cursor.execute("DROP INDEX IF EXISTS idx_A")
    cursor.execute("DROP INDEX IF EXISTS idx_A_B")
    conn.commit()
    cursor.execute("CREATE INDEX idx_A ON testIndex(A)")
    conn.commit()
    start_time = time.time()
    cursor.execute("SELECT B FROM testIndex WHERE A = 42")
    cursor.fetchall()
    interval_1 = time.time() - start_time
    print(f"With single-column index on A: {interval_1:.6f} seconds")

    # 组合索引
    cursor.execute("CREATE INDEX idx_A_B ON testIndex(A, B)")
    conn.commit()
    start_time = time.time()
    cursor.execute("SELECT B FROM testIndex WHERE A = 42")
    cursor.fetchall()
    interval_2 = time.time() - start_time
    print(f"With composite index on (A, B): {interval_2:.6f} seconds")
    print(f"ratio = {interval_1 / interval_2:.6f}")

    cursor.close()
    conn.close()

def experiment_3(db_path):
    # 针对 C 列字符串，观察函数索引的作用
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("\n[Experiment 3] 函数索引作用")
    # 无索引
    cursor.execute("DROP INDEX IF EXISTS idx_lower_C")
    conn.commit()
    start_time = time.time()
    cursor.execute("SELECT * FROM testIndex WHERE LOWER(C) = 'string_5000'")
    cursor.fetchall()
    interval_1 = time.time() - start_time
    print(f"Without function index on LOWER(C): {interval_1:.6f} seconds")

    # 创建函数索引
    cursor.execute("CREATE INDEX idx_lower_C ON testIndex(LOWER(C))")
    conn.commit()
    start_time = time.time()
    cursor.execute("SELECT * FROM testIndex WHERE LOWER(C) = 'string_5000'")
    cursor.fetchall()
    interval_2 = time.time() - start_time
    print(f"With function index on LOWER(C): {interval_2:.6f} seconds")
    print(f"ratio = {interval_1 / interval_2:.6f}")

    cursor.close()
    conn.close()