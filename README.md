# Installation

```bash
docker run --rm -p 8088:8088 -p 9870:9870 -p 9864:9864 -v /local/path/to/jars:/tmp/jars -v /local/path/to/data:/tmp/data --name hadoop-tp3-cont hadoop-tp3-img
```
```bash
docker exec -it hadoop-tp3-cont /bin/bash
```
```bash
hdfs dfs -mkdir -p /relationships
```
```bash
hdfs dfs -put /tmp/data/relationships/data.txt /relationships/
```


# Job 1 - [.jar](jars/tpfinal-remi_guerin_job1.jar)

```bash
hadoop jar /tmp/jars/tpfinal-remi_guerin_job1.jar /relationships/data.txt /output/job1
```
Résultats:
```bash
hdfs dfs -cat /output/job1/part-*
```
# Job 2 - [.jar](jars/tpfinal-remi_guerin_job2.jar)

```bash
hadoop jar /tmp/jars/tpfinal-remi_guerin_job2.jar /output/job1 /output/job2
```
Résultats:
```bash
hdfs dfs -cat /output/job2/part-*
```

# Job 3 - [.jar](jars/tpfinal-remi_guerin_job3.jar)

```bash
hadoop jar /tmp/jars/tpfinal-remi_guerin_job3.jar /output/job2 /output/job3
```
Résultats:
```bash
hdfs dfs -cat /output/job3/part-*
```