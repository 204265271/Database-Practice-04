import sqlite3
import random
import time
import matplotlib.pyplot as plt

def create_sessions_table(conn):
    """创建 sessions 表"""
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        start_time INTEGER NOT NULL,
        end_time INTEGER NOT NULL
    )
    ''')
    conn.commit()

def generate_data(conn, num_records):
    """生成模拟数据"""
    cursor = conn.cursor()
    cursor.execute("DELETE FROM sessions")  # 清空表
    for _ in range(num_records):
        start_time = random.randint(1, 1000000)
        end_time = start_time + random.randint(1, 1000)
        cursor.execute("INSERT INTO sessions (start_time, end_time) VALUES (?, ?)", (start_time, end_time))
    conn.commit()

def max_concurrent_sessions_set_based(conn):
    """基于集合的方法计算最大并发会话数"""
    cursor = conn.cursor()
    start_time = time.time()
    cursor.execute('''
    SELECT MAX(concurrent_sessions) AS max_concurrent
    FROM (
        SELECT COUNT(*) AS concurrent_sessions
        FROM sessions AS s1
        JOIN sessions AS s2
        ON s1.start_time <= s2.end_time AND s1.end_time >= s2.start_time
        GROUP BY s1.id
    )
    ''')
    result = cursor.fetchone()[0]
    elapsed_time = time.time() - start_time
    return result, elapsed_time

def max_concurrent_sessions_cursor_based(conn):
    """基于游标的方法计算最大并发会话数"""
    cursor = conn.cursor()
    start_time = time.time()
    cursor.execute("SELECT start_time, end_time FROM sessions ORDER BY start_time")
    sessions = cursor.fetchall()
    max_concurrent = 0
    current_concurrent = 0
    events = []
    for start, end in sessions:
        events.append((start, 1))  # 会话开始
        events.append((end, -1))  # 会话结束
    events.sort()  # 按时间排序
    for _, change in events:
        current_concurrent += change
        max_concurrent = max(max_concurrent, current_concurrent)
    elapsed_time = time.time() - start_time
    return max_concurrent, elapsed_time

def max_concurrent_sessions_window_function(conn):
    """基于窗口函数的方法计算最大并发会话数"""
    cursor = conn.cursor()
    start_time = time.time()
    cursor.execute('''
    WITH events AS (
        SELECT start_time AS time, 1 AS change FROM sessions
        UNION ALL
        SELECT end_time AS time, -1 AS change FROM sessions
    ),
    running_total AS (
        SELECT time, SUM(change) OVER (ORDER BY time) AS concurrent_sessions
        FROM events
    )
    SELECT MAX(concurrent_sessions) AS max_concurrent FROM running_total
    ''')
    result = cursor.fetchone()[0]
    elapsed_time = time.time() - start_time
    return result, elapsed_time

def run_experiment(db_file):
    """运行实验并生成性能报告"""
    conn = sqlite3.connect(db_file)
    create_sessions_table(conn)

    data_sizes = [1000, 2000, 5000, 10000, 20000, 30000, 50000]  # 数据规模
    set_based_times = []
    cursor_based_times = []
    window_function_times = []

    for size in data_sizes:
        print(f"\n[INFO] Running experiment for {size} records...")
        generate_data(conn, size)

        # 基于集合的方法
        _, set_time = max_concurrent_sessions_set_based(conn)
        set_based_times.append(set_time)

        # 基于游标的方法
        _, cursor_time = max_concurrent_sessions_cursor_based(conn)
        cursor_based_times.append(cursor_time)

        # 基于窗口函数的方法
        _, window_time = max_concurrent_sessions_window_function(conn)
        window_function_times.append(window_time)

    conn.close()
    
    for i in range(0, len(data_sizes)): 
        print(f"data_size = {data_sizes[i]}: set = {set_based_times[i]}, cursor = {cursor_based_times[i]}, window_function = {window_function_times[i]}")
        print(f"            ration = {set_based_times[i]/window_function_times[i]:.6f}:{cursor_based_times[i]/window_function_times[i]:.6f}:1")

    # 绘制性能对比图
    plt.figure(figsize=(10, 6))
    plt.plot(data_sizes, set_based_times, label="Set-Based", marker="o")
    plt.plot(data_sizes, cursor_based_times, label="Cursor-Based", marker="o")
    plt.plot(data_sizes, window_function_times, label="Window Function", marker="o")
    plt.xlabel("Number of Records")
    plt.ylabel("Execution Time (seconds)")
    plt.title("Performance Comparison of Different Methods")
    plt.legend()
    plt.grid()
    plt.show()