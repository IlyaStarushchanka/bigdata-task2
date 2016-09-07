package com.epam.bigdata.yarnapp;

import org.apache.hadoop.fs.Path;

public class Constants {
    /**
     * Environment key name pointing to the the app master jar location
     */
    public static final String AM_JAR_PATH = "AM_JAR_PATH";

    /**
     * Environment key name denoting the file timestamp for the shell script.
     * Used to validate the local resource.
     */
    public static final String AM_JAR_TIMESTAMP = "AM_JAR_TIMESTAMP";

    /**
     * Environment key name denoting the file content length for the shell script.
     * Used to validate the local resource.
     */
    public static final String AM_JAR_LENGTH = "AM_JAR_LENGTH";


    public static final String AM_JAR_NAME = "AppMaster.jar";

    public static final String FILE_DESTINATION = "hdfs://sandbox.hortonworks.com:8020/";

    public static final Path HDFS_MY_APP_JAR_PATH = new Path(Constants.FILE_DESTINATION + "/apps/bigdata2/bigdata-task2-1.0-SNAPSHOT-jar-with-dependencies.jar");

}
