import task1 
import os

def clear_db_files():
    # 获取当前文件所在目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 拼接 db_files 文件夹路径
    db_files_dir = os.path.join(current_dir, "db_files")
    # 检查 db_files 文件夹是否存在
    if os.path.exists(db_files_dir) and os.path.isdir(db_files_dir):
        # 遍历并删除文件夹中的所有文件
        for file_name in os.listdir(db_files_dir):
            file_path = os.path.join(db_files_dir, file_name)
            if os.path.isfile(file_path):  # 确保是文件
                os.remove(file_path)
                print(f"Deleted: {file_path}")
        print(f"All files in 'db_files' have been deleted.")
    else:
        print("'db_files' folder does not exist in the current directory.")

if __name__ == "__main__": 
    # firstly, clear the db_files folder for the new test
    clear_db_files()
    db_files_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "db_files")
    
    # task 1
    print() 
    print("         ### task 1 ###")
    print()
    db_file_1 = os.path.join(db_files_dir, "task1.db")
    print("db_file =", db_file_1)
    task1.create_and_populate_table(db_file_1)
    task1.experiment_1(db_file_1)
    task1.experiment_2(db_file_1)
    task1.experiment_3(db_file_1)