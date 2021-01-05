# SetupSpark
The Repository contains python code for SetupSpark Library which is useful to download and setup Apache Spark with Apache Hadoop Cluster (pseudo distributed mode with single machine).

The Library helps to set up spark cluster for local development and testing purposes of pyspark projects.
It assumes that JAVA is already available on the machine. It can be specified using JAVA_HOME Environment Variable or as an argument while running the setup process.

The Code is developed in Python with Poetry as a package manager

Follow the below steps to run the code in your local machine.

* Install Python (>3.6) and create a Virtual Environment with poetry as a dependency.
* Clone the Repository from Github.
* Run: poetry install to install the library to Virtual Environment
* Run: poetry run setupspark with required arguments

---

### For Help on How to Run the Setup
```
poetry run setupspark --help
```

---

### To Run the Setup
```
poetry run setupspark --spark-version=spark-version \
                      --spark-path=spark-path \
                      --hadoop-version=hadoop-version \
                      --hadoop \
                      --hadoop-path=hadoop-path \
                      -l=loglevel
                      
poetry run setupspark -c="setup_spark_args.txt"

Here setup_spark_args.txt contains the arguments one per each line. 
Please use only one of the above methods. 
when file is passed with --config option, then the remaining arguments are ignored.
```

### Sample contents for file setup_spark_args.txt

```
--spark-version=spark-version
--spark-path=spark-path
--hadoop-version=hadoop-version
--hadoop
--hadoop-path=hadoop-path
-l=INFO
--java-home=java-home-path
```