package com.mycompany.app;

import java.util.ArrayList;
import java.util.Arrays;
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
	private final static JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("document-rank"));
	private String DATA_DIR = "AssignmentData/";
	private String INPUT_DIR = "AssignmentData/datafiles/";

	public DocumentRank() {
	}

	/*
	 * Stage 1
	 */
	public JavaPairRDD<String, Integer> computeWordFreq() {
		JavaRDD<String> words = sc.textFile(DATA_DIR + "stopwords.txt");
		List<String> list = words.flatMap(s -> Arrays.asList(s.split(" ")).stream().map(str -> str.trim().toLowerCase())
				.collect(Collectors.toList()).iterator()).collect();
		Set<String> stopWords = new HashSet<String>(list);

		JavaPairRDD<String, String> files = sc.wholeTextFiles(INPUT_DIR);
		JavaPairRDD<String, Integer> counts = files.flatMap(
				tuple -> Arrays.asList(tuple._2.replaceAll("[^A-Za-z0-9']", " ").split(" ")).stream().filter(word -> {
					return !stopWords.contains(word.toLowerCase()) && !word.equals("");
				}).map(s -> s.trim() + "@"
						+ (tuple._1.charAt(tuple._1.length() - 6) == 'f'
								? tuple._1.substring(tuple._1.length() - 6, tuple._1.length() - 4)
								: tuple._1.substring(tuple._1.length() - 7, tuple._1.length() - 4)))
						.collect(Collectors.toList()).iterator())
				.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b);
		counts.saveAsTextFile("output_stage1");
		return counts;
	}

	/*
	 * Stage 2
	 */
	public JavaPairRDD<String, Double> computeTfIdf(JavaPairRDD<String, Integer> wordFreqCounts) {
		JavaPairRDD<String, Double> tfIdfResults = wordFreqCounts
				.mapToPair(tuple -> new Tuple2<>(tuple._1.split("@")[0], tuple._1.split("@")[1] + "=" + tuple._2))
				.groupByKey().flatMapToPair(pair -> {
					int count = Iterables.size(pair._2);
					List<Tuple2<String, Double>> list = new ArrayList<>();
					for (String s : pair._2) {
						String key = pair._1 + "@" + s.split("=")[0];
						int tf = Integer.parseInt(s.split("=")[1]);
						double val = (1 + Math.log(tf)) * Math.log(10.0 / count);
						list.add(new Tuple2<String, Double>(key, val));
					}
					return list.iterator();
				});
		tfIdfResults.saveAsTextFile("output_stage2");
		return tfIdfResults;
	}

	/*
	 * Stage 3
	 */
	public JavaPairRDD<String, Double> computeNormalisedTfIdf(JavaPairRDD<String, Double> tfIdfResults) {
		// Compute square sum of each document
		Map<String, Double> squareSum = tfIdfResults.mapToPair(tuple -> {
			String key = tuple._1.split("@")[1]; // f1
			String value = tuple._1.split("@")[0] + "=" + tuple._2; // word=3
			return new Tuple2<String, String>(key, value);
		}).groupByKey().mapToPair(pair -> {
			Double sum = 0.0;
			for (String s : pair._2) {
				sum += Math.pow(Double.parseDouble(s.split("=")[1]), 2);
			}
			return new Tuple2<String, Double>(pair._1, sum);
		}).collectAsMap();

		JavaPairRDD<String, Double> normalisedResults = tfIdfResults.mapToPair(tuple -> {
			String fileName = tuple._1.split("@")[1];
			Double sum = squareSum.get(fileName);
			return new Tuple2<String, Double>(tuple._1, tuple._2 / (Math.sqrt(sum)));
		});

		normalisedResults.saveAsTextFile("output_stage3");
		return normalisedResults;
	}

	/*
	 * Stage 4
	 */
	public JavaPairRDD<String, Double> relevanceScore(JavaPairRDD<String, Double> normalisedResults) {
		JavaRDD<String> query = sc.textFile(DATA_DIR + "query.txt");
		JavaPairRDD<String, Integer> list = query.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
				.mapToPair(keyword -> new Tuple2<String, Integer>(keyword.trim().toLowerCase(), 1));
		Map<String, Integer> keywordFreq = list.collectAsMap();
		JavaPairRDD<String, Double> relevanceScore = normalisedResults.mapToPair(tuple -> {
			String word = tuple._1.split("@")[0];
			String fileName = tuple._1.split("@")[1];
			return keywordFreq.keySet().contains(word.toLowerCase())
					? new Tuple2<String, Double>(fileName, tuple._2 * keywordFreq.get(word.toLowerCase()))
					: new Tuple2<String, Double>(fileName, 0.0);
		}).reduceByKey((a, b) -> a + b);

		relevanceScore.saveAsTextFile("output_stage4");
		return relevanceScore;
	}

	/*
	 * Stage 5
	 */
	public JavaRDD<Tuple2<String, Double>> documentSort(JavaPairRDD<String, Double> relevanceScore, int k) {
		List<Tuple2<Double, String>> temp = relevanceScore
				.mapToPair(tuple -> new Tuple2<Double, String>(tuple._2, tuple._1)).sortByKey(false).take(k);
		JavaRDD<Tuple2<String, Double>> topKDocs = sc.parallelize(temp)
				.map(tuple -> new Tuple2<String, Double>(tuple._2, tuple._1));
		topKDocs.saveAsTextFile("output_stage5");
		return topKDocs;
	}

	public static void main(String[] args) throws Exception {
		int k = 3;
		DocumentRank dr = new DocumentRank();
		JavaPairRDD<String, Integer> outStage1 = dr.computeWordFreq();
		JavaPairRDD<String, Double> outStage2 = dr.computeTfIdf(outStage1);
		JavaPairRDD<String, Double> outStage3 = dr.computeNormalisedTfIdf(outStage2);
		JavaPairRDD<String, Double> outStage4 = dr.relevanceScore(outStage3);

		dr.documentSort(outStage4, k);
	}
}
