Transformations, Actions, and Evaluation in Spark
Spark relies on a lazy execution model built around two core types of operations: Transformations and Actions. 
________________________________________
1. Transformations
Transformations are operations applied to a dataset (DataFrame or RDD) that create a brand-new dataset without altering the original one. They follow the principle of immutability.
•	No Immediate Execution: Calling a transformation does not compute the results. It simply registers the instruction in the DAG.
•	Categories: They are fundamentally split into two types based on how data moves across the cluster: Narrow and Wide. 
Narrow Transformations
A narrow transformation means each partition of the parent dataset is used by at most one partition of the child dataset. 
•	No Network Shuffle: Data stays within the same machine/executor. No data is sent over the network.
•	Pipelining: Spark can chain multiple narrow transformations together into a single Stage to process data efficiently in memory.
•	Examples: filter(), map(), flatMap(), select(), drop(). 
Wide Transformations
A wide transformation means multiple child partitions depend on data from multiple parent partitions. 
•	Network Shuffle Required: Data must be partitioned, sorted, and transferred across different nodes in the cluster.
•	Stage Boundary: Wide transformations force Spark to break the execution plan and create a brand-new Stage. This introduces disk I/O and network overhead.
•	Examples: groupBy(), join(), distinct(), repartition(), reduceByKey(). 
________________________________________
2. Lazy Evaluation
Lazy Evaluation means Spark does not execute data processing instructions immediately when they are written in the code. Instead, it waits until the absolute last moment to run them. [1, 2, 3]
•	Lineage Graph Building: When you chain transformations, Spark merely records them as a blueprint called a Lineage Graph (or DAG). 
•	Triggered by Actions: The physical execution is only kicked off when an Action is invoked. 
•	Why use it? (Interview Highlight):
o	Catalyst Optimizer: By waiting, Spark looks at the entire chain of commands and optimizes the physical execution plan.
o	Predicate Pushdown: If you load a massive dataset and filter it at the end, Spark’s optimizer will push the filter down to the data source level, loading only the required records into memory.
o	Memory Efficiency: It prevents intermediate data from being written to disk or memory needlessly. 
________________________________________
3. Actions
Actions are operations that instruct Spark to compute a result from the DAG and return it outside the Spark engine or write it to storage. 
•	Job Trigger: An action is the exact trigger that converts the logical DAG into a physical Job on the cluster.
•	Data Materialization: It forces the evaluation of all preceding transformations.
•	Output Destinations: The result is either sent back to the Driver program or written out to an external storage system (like HDFS, S3, or a database). 
•	Examples:
o	collect(): Brings the entire dataset back to the Driver (dangerous for large datasets as it can cause Out-Of-Memory errors).
o	count(): Returns the total number of rows.
o	first(), take(n): Fetches the first row or first \(n\) rows.
o	write() / save(): Writes the output to storage.
o	show(): Prints a sample of the data to the console. 
________________________________________
<img width="641" height="310" alt="image" src="https://github.com/user-attachments/assets/9083e50c-7e1e-43f0-99c8-c581820353db" />

