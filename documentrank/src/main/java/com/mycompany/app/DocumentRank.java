package com.mycompany.app;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public final class DocumentRank {
	private final static JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wordcount"));
	private List<String> QUERY_WORDS = new ArrayList<>();
	private List<String> STOP_WORDS = new ArrayList<>();
	private List<String> DATA_FILES = Arrays.asList("f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10");

	public DocumentRank() {
		JavaRDD<String> query = sc.textFile("AssignmentData/query.txt");
		JavaRDD<String> stopWords = sc.textFile("AssignmentData/stopwords.txt");
		QUERY_WORDS = query.collect();
		STOP_WORDS = stopWords.collect();
	}

	public void computeWordFrequencySingleDoc(String fileName) {
		JavaRDD<String> textFile = sc.textFile("AssignmentData/datafiles/" + fileName + ".txt");
		JavaPairRDD<String, Integer> counts = textFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
				.filter(word -> {
					return false;
				}).mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b);
		// TODO: filter stopwords
		counts.saveAsTextFile("stage1_output_" + fileName);

	}

	public void print() {
		System.out.println("*********************");
		for (int i = 0; i < QUERY_WORDS.size(); i++)
			System.out.println(' ' + QUERY_WORDS.get(i));
		System.out.println();
		for (int i = 0; i < STOP_WORDS.size(); i++)
			System.out.print(' ' + STOP_WORDS.get(i));
		System.out.println();

	}

	public static void main(String[] args) throws Exception {
		DocumentRank dr = new DocumentRank();
		dr.DATA_FILES.stream().forEach(file -> dr.computeWordFrequencySingleDoc(file));
	}
}
