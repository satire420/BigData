package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.CollectionAccumulator;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentfunctions.DPHScorerMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.DocLenExtractor;
import uk.ac.gla.dcs.bigdata.studentfunctions.DocLengthAggregator;
import uk.ac.gla.dcs.bigdata.studentfunctions.DocumentRankMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsAggregator;
import uk.ac.gla.dcs.bigdata.studentfunctions.ResultMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.TermContextMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.ZeroFrequencyFilter;
import uk.ac.gla.dcs.bigdata.studentstructures.DatasetCleaned;
import uk.ac.gla.dcs.bigdata.studentstructures.DocScoreAccumulator;
import uk.ac.gla.dcs.bigdata.studentstructures.TermContext;
import uk.ac.gla.dcs.bigdata.studentstructures.TermContextDPH;
import uk.ac.gla.dcs.bigdata.studentstructures.TermFrequencyAcc;


/**
 * This is the main class where your Spark topology should be specified.
 *
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */

public class AssessedExercise {

	public static void main(String[] args) {

		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it

		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null)
		 {
			sparkMasterDef = "local[2]"; // default is local mode with two executors
		}

		String sparkSessionName = "BigDataAE"; // give the session a name

		// Create the Spark Configuration
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);

		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();


		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null)
		 {
			queryFile = "data/queries.list"; // default is a sample with 3 queries
		}

		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
//		if (newsFile==null)
//		 {
//			newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
//		}
		if (newsFile==null)
		 {
			newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix.json"; // Original dataset of news articles
		}
		

	    List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);

	    // Close the spark session
	    spark.close();

	    if (results != null) {
	        File outDirectory = new File("results/" + System.currentTimeMillis());
	        if (!outDirectory.exists()) {
	            outDirectory.mkdirs();
	        }

	        // Ensure directories for each query exist before calling write
	        results.forEach(rankingForQuery -> {
	            // Sanitize query to create a valid directory name
	            String queryDirName = rankingForQuery.getQuery().getOriginalQuery().replace(" ", "_").replaceAll("[^a-zA-Z0-9_\\-]", "");
	            File queryDirectory = new File(outDirectory, queryDirName);
	            if (!queryDirectory.exists()) {
	                queryDirectory.mkdirs();
	            }
	            rankingForQuery.write(queryDirectory.getAbsolutePath());
	        });
	    } else {
	        System.err.println("Topology return no rankings, student code may not be implemented, skipping final write.");
	    }
	}




	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {

		//Combine all the query terms into a list of strings.
		CollectionAccumulator<String> allQueryTerms = spark.sparkContext().collectionAccumulator();
		
		
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article
		System.out.println("Loaded Queries: " + queriesjson.count());
		System.out.println("Loaded News Articles: " + newsjson.count());
		
		
		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(allQueryTerms), Encoders.bean(Query.class)); //  converts each row into a Query
		
		queries.show();
		
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); //  converts each row into a NewsArticle

		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		//----------------------------------------------------------------

		// Convert Query List to one dimension List
		System.out.println("we are converting query into query List");
		Set<String> allQueryTermsToSet = new HashSet<>();   //delete duplicate element
		allQueryTermsToSet.addAll(allQueryTerms.value());	//convert query into Set, remove duplicated terms
		List<String> allQueryTermsToList = new ArrayList<String>(allQueryTermsToSet);	//Convert back to List


		// Convert article into cleaned version of our dataset, DatasetCleaned is a news-article related class type after text processing
		Encoder<DatasetCleaned> newsArticleEncoder = Encoders.bean(DatasetCleaned.class);
		Dataset<DatasetCleaned> articles = news.map(new NewsAggregator(), newsArticleEncoder);

		// Fetching average document length in corpus
		long totalDocsInCorpus = articles.count();
		System.out.println(totalDocsInCorpus);

		// Mapping news-article doc_length to a docLengthMap
		Dataset<Long> docLength = articles.map(new DocLenExtractor(), Encoders.LONG());
		long docLenSUM = docLength.reduce(new DocLengthAggregator());	// Adding all the document Lengths
		double avgDocLengthInCorpus = docLenSUM / totalDocsInCorpus;	// Calculating the average Document Length
		System.out.println(avgDocLengthInCorpus);
		
		
		// Define an accumulator to compute the sum of terms and their corresponding frequencies.
		TermFrequencyAcc tf_Accumulator = new TermFrequencyAcc(new HashMap<>());
		spark.sparkContext().register(tf_Accumulator, "termFrequencyAccumulator");


		//Broadcast allQueryTermsToList
		System.out.println("We are currently broadcasting!");

		//Broadcast allQueryTermsToList, which is a list of all query terms without duplicate
		Broadcast<List<String>> brdcastAllTermsToList = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(allQueryTermsToList);
		List<String> allTermsToList = brdcastAllTermsToList.value(); //no data found

		// Printing the contents of the list
		System.out.println("Contents of brdcastAllTermsToList:");
		for (String term : allTermsToList) {
		    System.out.println(term);
		}
		
		//Broadcast brdcastTotalDocs and broadcastAvgDocLength
        Broadcast<Long> broadcastTotalDocsInCorpus = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(totalDocsInCorpus);
        Broadcast<Double> broadcastAverageDocumentLengthInCorpus = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(avgDocLengthInCorpus);
        System.out.println("Data in total: "+broadcastTotalDocsInCorpus);
        
        //Term-context-map
        System.out.println("Mapping to termArticle!");
        Encoder<TermContext> termArticleEncoder= Encoders.bean(TermContext.class);

        //Convert DatasetCleaned to TermContexts, use brdcastAllTermsToList to get each term in Query
        Dataset<TermContext> termContexts = articles.flatMap(new TermContextMap(brdcastAllTermsToList), termArticleEncoder);
        System.out.println("termContext count before zero frquency filtering:" + termContexts.count());
        termContexts.show();
        
        // Excluding term-article pairs with zero frequency values, which is an essential optimization step.
        // Mapping pairs with non-zero frequencies to a new Dataset<TermContext> named filteredTermArticles.
        System.out.println("Filtering termContext that has zero frequency！");
        ZeroFrequencyFilter frequencyZeroFilter = new ZeroFrequencyFilter(tf_Accumulator);
        Dataset<TermContext> filteredTermArtcles = termContexts.flatMap(frequencyZeroFilter,termArticleEncoder);
        System.out.println("TermArticle numbers after filering:" + filteredTermArtcles.count());  //Get the count after filtering
        filteredTermArtcles.show();

        //broadcast TermandFrequency map to calculate DPH score
        Map<String, Integer> termFrequencyMap = tf_Accumulator.value();
        Broadcast<Map<String, Integer>> broadcastTermFrequencyMap = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(termFrequencyMap);


        // Creates an accumulator to collect document scores, avoiding data loss when converting datasets to lists for broadcasting.
  		DocScoreAccumulator scoreAccumulator = new DocScoreAccumulator(new HashMap<>());
  		spark.sparkContext().register(scoreAccumulator, "scoreAccumulator");

  		//DPH term-Article, we map the filteredTermArticle to TermContextDPH map
		System.out.println("Calculating DPH score！");
		Encoder<TermContextDPH> dphEncoder = Encoders.bean(TermContextDPH.class);
		Dataset<TermContextDPH> TermContextDPH = filteredTermArtcles.map(new DPHScorerMap(broadcastTermFrequencyMap,broadcastTotalDocsInCorpus,
				broadcastAverageDocumentLengthInCorpus, scoreAccumulator), dphEncoder);
		TermContextDPH.count();


		//create score Accumulator is just as same as TermContextDPH
		Map<Tuple2<String,NewsArticle>, Double> scoreMap = scoreAccumulator.value();
		Broadcast<Map<Tuple2<String,NewsArticle>, Double>> broadcastScoreMap = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(scoreMap);

		//it's a final step to get average score and rank them
		System.out.println("Combining terms into query, followed by Ranking them!");
		DocumentRankMap docrankmap = new DocumentRankMap(broadcastScoreMap);
		Dataset<DocumentRanking> docrank = queries.map(docrankmap, Encoders.bean(DocumentRanking.class));


		//get the final result
		System.out.println("Ranking the final result");
		Dataset<DocumentRanking> finalDocRank = docrank.map(new ResultMap(), Encoders.bean(DocumentRanking.class));
		System.out.println(finalDocRank.count());
		List<DocumentRanking> finalRankedDocs = finalDocRank.collectAsList();


		// Returning the final list of ranked documents as per our topology
		return finalRankedDocs ;
	}
}
