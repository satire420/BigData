package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
 
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.CollectionAccumulator;

import uk.ac.gla.dcs.bigdata.studentstructures.TermContext;
import uk.ac.gla.dcs.bigdata.studentstructures.TermFrequencyAcc;


public class ZeroFrequencyFilter implements FlatMapFunction<TermContext,TermContext>{
	
		// We aim to exclude term articles with a frequency value of zero.	
	
		private static final long serialVersionUID = -5421918183346003486L;
		boolean frquencyZero;
		TermFrequencyAcc tf_Accumulator;
		
		public ZeroFrequencyFilter() {
			// TODO Auto-generated constructor stub
		}
		
	 
		public ZeroFrequencyFilter(TermFrequencyAcc tf_Accumulator) {
			super();
			this.tf_Accumulator = tf_Accumulator;
			// TODO Auto-generated constructor stub
		}
	 
	 
		public Iterator<TermContext> call(TermContext value) throws Exception {
			
			frquencyZero = true;
			short frequency = value.getFrequency();
			Map<String, Integer> freqMap=new HashMap<String, Integer>();	
			
			if(frequency > 0) this.frquencyZero = false;
				
			
			if (!this.frquencyZero) {//if frequency is not 0 , then create a new TermArticle
				List<TermContext> termArticleList = new ArrayList<TermContext>(1);
				termArticleList.add(value);
				freqMap.put(value.getTerm(), (int) frequency);
				tf_Accumulator.add(freqMap);// It will fetch the similar key-value pair
				return termArticleList.iterator();
			} else {                        //if frequency is not 0 -> return null
				List<TermContext> termArticleList = new ArrayList<TermContext>(0);
				return termArticleList.iterator();
			}
		}

}
