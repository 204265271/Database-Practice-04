import sqlite3 
import os 

def test(): 
    print("the path is: ", os.path.abspath(__file__))
    print("the path exists main.py?: ", os.path.exists("main.py"))
