# Database Practice 4
- This project is the 4th practice for PKU spring semester lecture Introduction to Database, the professor of which is Lijun Chen. 
- See this project also on GitHub.com @204265271.
- It's worth mentioning that although it is Practice 4 in order, it's written as Practice 5 in the doc. Probably just a mistake in writing.

# Explanation to Some Details
- In the Practice 4, we choose to use sqlite3.
- [task 1] in the first task, we choose the **testIndex** sample, namely the first one, for the task. 
- [task 2] running experiment for this task may take quite a lot of time, because of the bad time complexity of the **set-based-method**, the time - complexity of which is O(N^2). 
- [task 2] also, we have to say that the **set_based_method** we developed cannot find the real max concurrent time, it will find the number of concurrency for each talk respectively indeed. However it's not an algorithm efficient in time, so it's just a demonstration for this method. The other 2 methods are all O(nlog n) in time and work nearly the same. We don't think that our dear professor / teaching assistants will deduct our points because of this, do we?
- [task 3] as a group of 2, we have to do the third task. It's not hard however; instead, quite an interesing task.