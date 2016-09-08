# bigdata-task2 "Yarn app"

###Clear all caches
```
rm -rf /hadoop/yarn/local/usercache/*
rm -rf /hadoop/yarn/local/filecache/*
rm -f /root/IdeaProjects/bigdata-task2/target/bigdata-task2-1.0-SNAPSHOT-jar-with-dependencies.jar
```
###Create folder "bigdata2" on "apps" in hdfs if you don't have this folder

###Delete jar from hdfs if you are copied jar to "/apps/bigdata2" folder before
```
hadoop fs -rm -skipTrash /apps/bigdata2/bigdata-task2-1.0-SNAPSHOT-jar-with-dependencies.jar
```

###Build project 
```
mvn clean install
```

###Copy jar from local folder to hdfs
```
/usr/hdp/2.4.0.0-169/hadoop/bin/hadoop fs -copyFromLocal /root/IdeaProjects/bigdata-task2/target/bigdata-task2-1.0-SNAPSHOT-jar-with-dependencies.jar /apps/bigdata2/bigdata-task2-1.0-SNAPSHOT-jar-with-dependencies.jar
```

###Run

```
yarn jar ${PathToProject}/bigdata-task2/target/bigdata-task2-1.0-SNAPSHOT-jar-with-dependencies.jar com.epam.bigdata.yarnapp.Client <in> <num_containers> <pathToJar>
```

Where

`in` - path to input file,

`num_containers` - number of containers,

`pathToJar` - path to jar file in hdfs.

For example:

```
yarn jar /root/IdeaProjects/bigdata-task2/target/bigdata-task2-1.0-SNAPSHOT-jar-with-dependencies.jar com.epam.bigdata.yarnapp.Client tmp/admin/user.profile.tags.us.min.txt 3 hdfs://sandbox.hortonworks.com:8020/apps/bigdata2/bigdata-task2-1.0-SNAPSHOT-jar-with-dependencies.jar
```
