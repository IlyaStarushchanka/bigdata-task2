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
    private static Configuration conf;
    private static FileSystem fileSystem;

    public static void initFileHelper() throws URISyntaxException, IOException {
        conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        fileSystem = FileSystem.get(new URI("hdfs://sandbox.hortonworks.com:8020"), conf);
    }

    public static int getLinesCount (String filePath) throws IOException, URISyntaxException {
        Path path = new Path(Constants.FILE_DESTINATION + filePath);
        initFileHelper();
        BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
        int linesCount = 0;
        while (br.readLine() != null) {
            linesCount++;
        }
        return linesCount - 1;
    }

    public static List<String> getLinesFromFile(String filePath, int offset, int count) throws URISyntaxException, IOException {
        List<String> lines = new ArrayList<String>();
        initFileHelper();
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
        BufferedWriter brOut = new BufferedWriter(new OutputStreamWriter(fileSystem.create(ptOut,true)));

        brOut.write(topLine);
        brOut.write("\n");
        for (int i = 0; i <= lines.size()-1; i++){
            String currentLine = lines.get(i);
            String totalWords = "";
            List<String> words = totalTopWords.get(i);
            for (int j = 0; j < words.size(); j++){
                if (j > 0){
                    totalWords += ",";
                }
                totalWords += words.get(j);
            }
            String text = currentLine.replaceFirst("\\s", " " + totalWords);
            brOut.write(text);
            brOut.write("\n");
        }
        brOut.close();
    }

}
