Architecture
•	Spark Architecture 
•	Driver 
•	Executor 
•	Cluster Manager 
•	DAG 
•	Task 
•	Stage 
•	Job 
Spark Architecture Overview
Apache Spark follows a master-slave architecture designed for distributed data processing. It relies on a central coordinator (Driver) that manages a distributed set of workers (Executors) across a machine cluster. 
________________________________________
1. Driver
The Driver is the master node process that runs the main() method of your application. It acts as the central orchestrator of the entire Spark application. 
•	Creates SparkSession: It initializes the SparkContext or SparkSession, which serves as the entry point to Spark.
•	Converts Code to Tasks: It translates user code (DataFrames, RDDs, SQL) into logical execution plans.
•	Schedules Work: It splits the execution plan into stages and tasks, then schedules them across executors.
•	Tracks Metadata: It monitors the health, progress, and location of running executors.
•	Returns Results: It aggregates final status data or partitions of data to return to the user. 
________________________________________
2. Executor
An Executor is a worker node process responsible for running the actual data processing tasks. 
•	Executes Tasks: It runs the individual tasks assigned to it by the Driver.
•	In-Memory Storage: It stores cached data in its internal heap or off-heap memory.
•	Reports Status: It constantly sends heartbeats and task execution metrics back to the Driver.
•	Isolation: Executors run as isolated JVM processes on worker nodes; a failure in one executor does not crash the entire application. 
________________________________________
3. Cluster Manager
The Cluster Manager is an external service responsible for acquiring and allocating resources across the physical cluster. 
•	Pluggable Resource Allocator: Spark can plug into different managers depending on infrastructure needs. 
•	Common Types:
o	Standalone: Spark’s built-in, simple cluster manager.
o	Apache YARN: The default resource manager used in Hadoop ecosystems.
o	Kubernetes: A cloud-native container orchestration tool popular for modern cloud deployments. 
•	Workflow: The Driver requests CPU cores and memory from the Cluster Manager, which then provisions Executors on worker nodes. 
________________________________________
4. DAG (Directed Acyclic Graph)
A DAG is the logical execution plan created by the Driver whenever a Spark application runs. 
•	Directed: The sequence moves strictly forward from one data operation to the next.
•	Acyclic: The sequence contains no loops; data flows linearly or branches out, but never loops back.
•	Graph: It maps out the exact lineage of operations (transformations) applied to the data.
•	Lazy Evaluation Trigger: Spark does not execute transformations instantly. It builds this graph until an Action (like .count(), .collect(), or .save()) is called, optimizing the whole graph before execution. 
________________________________________
5. Job
A Job is the highest level of execution in Spark’s runtime hierarchy.
•	Action Driven: Every single action called in your code spawns exactly one Spark Job.
•	Code Example: If your script loads data, filters it, runs .count(), and then runs .write(), Spark will generate exactly two distinct Jobs.
•	Structure: A Job represents the complete path required to return the result of that specific action, and it gets broken down into multiple Stages
________________________________________
6. Stage
A Stage represents a collection of tasks that can run in parallel on different data partitions using the same computation logic. [1, 2, 3]
•	Shuffle Boundaries: Spark splits Jobs into new Stages whenever a Shuffle operation (wide transformation) occurs. [1, 2]
•	Narrow vs Wide Transformations:
o	Narrow (Same Stage): Operations like map(), filter(), and rename() do not move data across network nodes. Data stays in place.
o	Wide (New Stage): Operations like groupBy(), join(), and distinct() require data redistribution across the network, forcing a Stage boundary. 
•	Pipeline Execution: Within a single Stage, Spark pipelines narrow operations together to save disk read/write overhead. 
________________________________________
7. Task
A Task is the smallest unit of work in Spark. It represents a single unit of execution sent to one Executor. [1, 2]
•	One-to-One with Partitions: One Task is launched for exactly one partition of data. If a Stage needs to process 100 data partitions, Spark creates 100 identical Tasks for that Stage. [1, 2, 3]
•	Execution Unit: Tasks require 1 CPU slot (thread) inside an Executor process to run. [1, 2]
•	Data Locality: Spark tries to launch Tasks on Executors that are physically closest to where the data partition resides (e.g., on the same machine or rack) to minimize network latency. [1, 2, 3]
________________________________________
Summary of the Structural Hierarchy
text
Spark Application
  └── Job (Triggered by an Action)
        └── Stage (Divided by Shuffles / Wide Transformations)
              └── Task (Executed per Data Partition inside an Executor)
