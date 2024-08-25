package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.HashMap;
import java.util.Map;
 
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
 
import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.DocScoreAccumulator;
import uk.ac.gla.dcs.bigdata.studentstructures.TermContext;
import uk.ac.gla.dcs.bigdata.studentstructures.TermContextDPH;

public class DPHScorerMap implements MapFunction<TermContext, TermContextDPH> {
	
	private static final long serialVersionUID = -4631167868446469099L;
	
	//Global Data
	Broadcast<Map<String, Integer>> broadcastTermFrequencyMap;
	Broadcast<Long> broadcastTotalDocsInCorpus;
	Broadcast<Double> broadcastAverageDocumentLengthInCorpus;
	DocScoreAccumulator scoreAccumulator;
 
		
	public DPHScorerMap(Broadcast<Map<String, Integer>> broadcastTermFrequencyMap, 
			Broadcast<Long> broadcastTotalDocsInCorpus, Broadcast<Double> broadcastAverageDocumentLengthInCorpus,
			DocScoreAccumulator scoreAccumulator) {
		super();
		this.broadcastTermFrequencyMap = broadcastTermFrequencyMap;
		this.broadcastTotalDocsInCorpus = broadcastTotalDocsInCorpus;
		this.broadcastAverageDocumentLengthInCorpus = broadcastAverageDocumentLengthInCorpus;
		this.scoreAccumulator = scoreAccumulator;
	}
 
 
	@Override
	public  TermContextDPH  call(TermContext value) throws Exception {
		
		
	short termFrequencyInCurrentDocument = 0;
	int totalTermFrequencyInCorpus = 0;
	int currentDocumentLength = 0;
	double averageDocumentLengthInCorpus = 0;
	long totalDocsInCorpus = 0;
	String term = "";
	NewsArticle article = new NewsArticle();
		
	term = value.getTerm();
	article = value.getArticle().getOriginalArticle();
 
	//get termFrequencyInCurrentDocument
	termFrequencyInCurrentDocument = value.getFrequency();
	
	//get totalTermFrequencyInCorpus
	Map<String, Integer> termAndFrequencyMap = broadcastTermFrequencyMap.value();
	if(termAndFrequencyMap.get(term)!=null) totalTermFrequencyInCorpus = termAndFrequencyMap.get(term);
	
	//get averageDocumentLengthInCorpus
	averageDocumentLengthInCorpus = broadcastAverageDocumentLengthInCorpus.value();
 
	//get totalDocsInCorpus
	totalDocsInCorpus = broadcastTotalDocsInCorpus.value();
	
	//get currentDocumentLength
	currentDocumentLength = value.getArticle().getDoc_length().intValue();
		
 
	double DPHsocre= DPHScorer.getDPHScore(termFrequencyInCurrentDocument, totalTermFrequencyInCorpus, 
			currentDocumentLength, averageDocumentLengthInCorpus, totalDocsInCorpus);
		TermContextDPH allResults = new  TermContextDPH (DPHsocre, term, article);
		
	//use scoreAcuumulator
	Map<Tuple2<String, NewsArticle>, Double> scoreMap=new HashMap<Tuple2<String,NewsArticle>, Double>();
	Tuple2<String, NewsArticle> key = new Tuple2<String, NewsArticle>(term,article);
	scoreMap.put(key, DPHsocre);
	scoreAccumulator.add(scoreMap);
	//	System.out.println(key._1+key._2+DPHsocre);
 
	return allResults;
	}
}
