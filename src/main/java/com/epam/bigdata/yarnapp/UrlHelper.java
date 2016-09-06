package com.epam.bigdata.yarnapp;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Ilya_Starushchanka on 9/6/2016.
 */
public class UrlHelper {

    private static String urlPatternString = "http[s]*:[^\\s\\r\\n]+";
    private static String urlTextPatternString = "(?<=((http|https)://www.miniinthebox.com/)).*(?=_.*)";

    public static List<String> parseUrl(List<String> lines/*, int offset, int count*/){
        Pattern urlPattern = Pattern.compile(urlPatternString);
        List<String> urls = new ArrayList<>();
        for (String line : lines) {
            Matcher urlMatcher = urlPattern.matcher(line);
            urlMatcher.matches();
            while (urlMatcher.find()) {
                urls.add(urlMatcher.group());
            }
        }
        //}
        return urls;
    }

    public static String getTextFromUrl (String url){
        String text = "";
        try {
            text = Jsoup.connect(url).get().text();
            text = Jsoup.parse(text).text();
        } catch (IOException e) {
            System.out.println("Can't connect to " + url);
            Pattern urlTextPattern = Pattern.compile(urlTextPatternString);
            Matcher urlTextMatcher = urlTextPattern.matcher(url);
            if (urlTextMatcher.find()){
                text = urlTextMatcher.group();
            }
            text = text.replaceAll("-", " ");
        }
        return text;
    }

}
