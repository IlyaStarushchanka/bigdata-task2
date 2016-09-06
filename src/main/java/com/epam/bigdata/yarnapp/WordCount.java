package com.epam.bigdata.yarnapp;

import java.io.*;
import java.net.*;
import java.net.URL;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

public class WordCount {
    private int offset;
    private int count;
    private String inputFile;
    private List<String> stopWords = Arrays.asList("a", "and", "for", "to", "the", "you", "in");

    public WordCount() {
        System.out.println("WordCount!");
    }

    public WordCount(String inputFile, int offset, int count) {
        this.inputFile = inputFile;
        this.offset = offset;
        this.count = count;
        System.out.println("WordCount!");
    }

    public void searchWords() {
        try {
            List<String> lines = FileHelper.getLinesFromFile(inputFile, offset, count);
            List<String> urls = UrlHelper.parseUrl(lines);
            List<List<String>> totalTopWords = new ArrayList<>();
            for (String url : urls) {
                System.out.println(url);

                String text = UrlHelper.getTextFromUrl(url);

                List<String> words = Pattern.compile("\\W").splitAsStream(text)
                        .filter((s -> !s.isEmpty()))
                        .filter(w -> !Pattern.compile("\\d+").matcher(w).matches())
                        .filter(w -> !stopWords.contains(w))
                        .collect(toList());

                List<String> topWords = words.stream()
                        .map(String::toLowerCase)
                        .collect(groupingBy(Function.identity(), counting()))
                        .entrySet().stream()
                        .sorted(Map.Entry.<String, Long>comparingByValue(reverseOrder()).thenComparing(Map.Entry.comparingByKey()))
                        .limit(10)
                        .map(Map.Entry::getKey)
                        .collect(toList());
                totalTopWords.add(topWords);
            }
            FileHelper.writeLinesToFile(Constants.FILE_DESTINATION + inputFile + "part" + offset, lines, totalTopWords);
        } catch (IOException e){
            System.out.println("Some problems with reading or writing file: " + e.getMessage());
        } catch (URISyntaxException e) {
            System.out.println("Some problems with path to file: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        String inputFile = args[0];
        String offset = args[1];
        String count = args[2];
        WordCount wordCount = new WordCount(inputFile, Integer.valueOf(offset), Integer.valueOf(count));
        wordCount.searchWords();
    }
}
