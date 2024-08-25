package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
 
import org.apache.commons.net.nntp.Article;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;

import uk.ac.gla.dcs.bigdata.studentstructures.DatasetCleaned;
import uk.ac.gla.dcs.bigdata.studentstructures.TermContext;

public class TermContextMap implements FlatMapFunction<DatasetCleaned,TermContext> {
	
	//We are using flatMap to map NewsArticle with TermContext
	
	private static final long serialVersionUID = 100L;
	
	Broadcast<List<String>> broadcastTermsList;
	
	public  TermContextMap(Broadcast<List<String>> broadcastTermsList) {
		this.broadcastTermsList = broadcastTermsList;
	}
 
	@Override
	public Iterator<TermContext> call(DatasetCleaned article) throws Exception {
		List<TermContext> termArticle = new ArrayList<>();
		
		// Get the broadcast value of term list
		
		List<String> termsList = broadcastTermsList.value();
		
		// Generating a new Term Article for each term in the list.
		for (String term : termsList) {
			termArticle.add(new TermContext(term, article));
        }
		return termArticle.iterator();
	}

}
