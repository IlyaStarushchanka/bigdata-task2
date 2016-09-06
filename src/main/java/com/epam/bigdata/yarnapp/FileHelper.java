package com.epam.bigdata.yarnapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Ilya_Starushchanka on 9/6/2016.
 */
public class FileHelper {

    public static Configuration conf;
    public static FileSystem fileSystem;
    public static String topLine;

    public static void initFileHelper() throws URISyntaxException, IOException {
        conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        fileSystem = FileSystem.get(new URI("hdfs://sandbox.hortonworks.com:8020"), conf);
    }

    public static int getLinesCount (String filePath) throws IOException {
        Path path = new Path(Constants.FILE_DESTINATION + filePath);
        BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
        int linesCount = 0;
        while (br.readLine() != null) {
            linesCount++;
        }
        return linesCount - 1;
    }

    public static List<String> getLinesFromFile(String filePath, int offset, int count) {
        List<String> lines = new ArrayList<String>();
        try {
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

            FileSystem fsOut = FileSystem.get(new URI("hdfs://sandbox.hortonworks.com:8020"),conf);
            Path path = new Path(Constants.FILE_DESTINATION + filePath);
            BufferedReader br = new BufferedReader(new InputStreamReader(fsOut.open(path)));

            int currentLine = 0;
            String line = br.readLine();
            topLine = line;
            line = br.readLine();
            while (line != null || currentLine < offset + count) {
                if (currentLine >= offset) {
                    lines.add(line.trim());
                }
                line = br.readLine();
                currentLine++;
            }
        } catch (IOException e){
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return lines;
    }

}
