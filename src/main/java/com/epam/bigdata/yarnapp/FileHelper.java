package com.epam.bigdata.yarnapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Ilya_Starushchanka on 9/6/2016.
 */
public class FileHelper {

    public static String topLine;

    public static int getLinesCount (String filePath) throws IOException, URISyntaxException {
        Path path = new Path(Constants.FILE_DESTINATION + filePath);
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://sandbox.hortonworks.com:8020"), conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
        int linesCount = 0;
        while (br.readLine() != null) {
            linesCount++;
        }
        return linesCount - 1;
    }

    public static List<String> getLinesFromFile(String filePath, int offset, int count) throws URISyntaxException, IOException {
        List<String> lines = new ArrayList<String>();
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://sandbox.hortonworks.com:8020"), conf);
        Path path = new Path(Constants.FILE_DESTINATION + filePath);
        BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(path)));

        int currentLine = 0;
        String line = br.readLine();
        topLine = line;
        line = br.readLine();
        while (line != null && currentLine < offset + count) {
            if (currentLine >= offset) {
                lines.add(line.trim());
            }
            line = br.readLine();
            currentLine++;
        }
        return lines;
    }

    public static void writeLinesToFile(String path, List<String> lines, List<List<String>> totalTopWords) throws URISyntaxException, IOException {
        Path ptOut=new Path(path);
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        FileSystem fsOut = FileSystem.get(new URI("hdfs://sandbox.hortonworks.com:8020"),conf);
        //FileSystem fsOut = FileSystem.get(new Configuration());
        BufferedWriter brOut = new BufferedWriter(new OutputStreamWriter(fsOut.create(ptOut,true)));

        brOut.write(topLine);
        brOut.write("\n");
        System.out.println("STEP 4");
        for (int i = 0; i <= lines.size()-1; i++){
            String currentLine = lines.get(i);
            String totalWords = totalTopWords.get(i).toString();
            totalWords = totalWords.replaceAll("(\\s|\\[|\\])", "");
            String text = currentLine.replaceFirst("\\s", " " + totalWords);
            brOut.write(text);
            brOut.write("\n");
        }

        System.out.println("STEP 5");
        brOut.close();
    }

}
