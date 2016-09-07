#!/bin/sh
#
rm -rf /hadoop/yarn/local/usercache/*
rm -rf /hadoop/yarn/local/filecache/*
rm -f /root/IdeaProjects/bigdata-task2/target/bigdata-task2-1.0-SNAPSHOT-jar-with-dependencies.jar
#
hadoop fs -rm -skipTrash /apps/bigdata2/bigdata-task2-1.0-SNAPSHOT-jar-with-dependencies.jar
#
/usr/java/jdk1.8.0_101/bin/java -Dmaven.home=/usr/lib/idea-IC-162.1628.40/plugins/maven/lib/maven3 -Dclassworlds.conf=/usr/lib/idea-IC-162.1628.40/plugins/maven/lib/maven3/bin/m2.conf -Didea.launcher.port=7533 -Didea.launcher.bin.path=/usr/lib/idea-IC-162.1628.40/bin -Dfile.encoding=UTF-8 -classpath /usr/lib/idea-IC-162.1628.40/plugins/maven/lib/maven3/boot/plexus-classworlds-2.4.jar:/usr/lib/idea-IC-162.1628.40/lib/idea_rt.jar com.intellij.rt.execution.application.AppMain org.codehaus.classworlds.Launcher -Didea.version=2016.2.2 clean install
#
/usr/hdp/2.4.0.0-169/hadoop/bin/hadoop fs -copyFromLocal /root/IdeaProjects/bigdata-task2/target/bigdata-task2-1.0-SNAPSHOT-jar-with-dependencies.jar /apps/bigdata2/bigdata-task2-1.0-SNAPSHOT-jar-with-dependencies.jar
#
yarn jar /root/IdeaProjects/bigdata-task2/target/bigdata-task2-1.0-SNAPSHOT-jar-with-dependencies.jar com.epam.bigdata.yarnapp.ClientNew tmp/admin/user.profile.tags.us.min.txt 4 hdfs://sandbox.hortonworks.com:8020/apps/bigdata2/bigdata-task2-1.0-SNAPSHOT-jar-with-dependencies.jar