package com.mycompany.app;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Iterables;

import scala.Tuple2;

public final class DocumentRank {
	private final static JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wordcount"));
	private List<String> QUERY_WORDS = new ArrayList<>();
	private String DATA_DIR = "AssignmentData/";
	private String INPUT_DIR = "AssignmentData/datafiles/";
	// private Set<String> STOP_WORDS;
	private List<String> DATA_FILES = Arrays.asList("f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10");

	public DocumentRank() {
		JavaRDD<String> query = sc.textFile(DATA_DIR + "query.txt");
		QUERY_WORDS = query.collect();
	}

	public Map<String, JavaPairRDD<String, Integer>> computeWordFreq() {
		Map<String, JavaPairRDD<String, Integer>> wordFreqCounts = new HashMap<>();
		JavaRDD<String> stopWords = sc.textFile(DATA_DIR + "stopwords.txt");
		Set<String> STOP_WORDS = new HashSet<String>(stopWords.collect());
		DATA_FILES.stream()
				.forEach(fileName -> wordFreqCounts.put(fileName, computeWordFreqSingleDoc(fileName, STOP_WORDS)));
		return wordFreqCounts;
	}

	/*
	 * Stage 1
	 */
	public JavaPairRDD<String, Integer> computeWordFreqSingleDoc(String fileName, Set<String> STOP_WORDS) {
		// TODO: add try catch to this block
		JavaRDD<String> textFile = sc.textFile(INPUT_DIR + fileName + ".txt");
		// replace non-alphanumeric values with space
		JavaPairRDD<String, Integer> counts = textFile
				.flatMap(line -> Arrays.asList(line.replaceAll("[^A-Za-z0-9]", " ").split(" ")).stream()
						.map(s -> s.trim()).collect(Collectors.toList()).iterator())
				// remove stop words and empty word
				.filter(word -> {
					return !STOP_WORDS.contains(word.toLowerCase()) && !word.equals("");
				}).mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b);
		counts.saveAsTextFile("stage1/output_" + fileName);
		return counts;
	}

	/*
	 * Alternative
	 */
	public JavaPairRDD<String, Integer> computeWordFreq2() {
		JavaRDD<String> stopWords = sc.textFile(DATA_DIR + "stopwords.txt");
		Set<String> STOP_WORDS = new HashSet<String>(stopWords.collect());

		JavaPairRDD<String, String> files = sc.wholeTextFiles(INPUT_DIR);
		JavaPairRDD<String, Integer> counts = files.flatMap(tuple -> Arrays
				.asList(tuple._2.replaceAll("[^A-Za-z0-9]", " ").split(" ")).stream().map(s -> s.trim() + "@"
				// TODO: change to use regex
						+ (tuple._1.charAt(tuple._1.length() - 6) == 'f'
								? tuple._1.substring(tuple._1.length() - 6, tuple._1.length() - 4)
								: tuple._1.substring(tuple._1.length() - 7, tuple._1.length() - 4)))
				.collect(Collectors.toList()).iterator())
				// remove stop words and empty word
				.filter(word -> {
					return !STOP_WORDS.contains(word) && !word.equals("");
				}).mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b);

		counts.saveAsTextFile("output_stage1");
		return counts;
	}

	public List<String> lineSplit(String line) {
		return Arrays.asList(line.replaceAll("/[^A-Za-z0-9 ]/", " ").split(" ")).stream().map(s -> s.trim())
				.collect(Collectors.toList());
	}

	/*
	 * Stage 2
	 */
	public void computeTfIdf(JavaPairRDD<String, Integer> wordFreqCounts) {
		// JavaPairRDD<Object, Object> tfIdf =
		wordFreqCounts.mapToPair(tuple -> new Tuple2<>(tuple._1.split("@")[0], tuple._1.split("@")[1] + "=" + tuple._2))
				.groupByKey().flatMap(pair -> {
					int count = Iterables.size(pair._2);
					List<Tuple2<String, Integer>> list = new ArrayList<>();
					for (String s: pair._2) {
						String key = pair._1+"@"+s.split("=")[0];
						int tf = Integer.parseInt(s.split("=")[1]);
						double value = (1+Math.log(tf))*Math.log(10.0/count);
						list.add(new Tuple2<String, Doubble>(key, value));
					}
					
					return l;
				});
		// saveAsTextFile("output_stage2");
	}

	public static void main(String[] args) throws Exception {
		DocumentRank dr = new DocumentRank();
		JavaPairRDD<String, Integer> x = dr.computeWordFreq2();
		dr.computeTfIdf(x);
	}
}
