package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.HashMap;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class TermContext implements Serializable{
	
	private static final long serialVersionUID = 7860296794078492249L;
		
		
	String term;
	DatasetCleaned article;
	short frequency = 0;
	
	public TermContext() {
		// TODO Auto-generated constructor stub
	}
	//	Get the word map after text processing
	public TermContext(String term, DatasetCleaned article) {
		super();
		this.term = term;
		this.article = article;
		setFrequency();//get the word map after text processing
	}
	
 
	public String getTerm() {
		return term;
	}
	public void setTerm(String term) {
		this.term = term;
	}
	public DatasetCleaned getArticle() {
		return article;
	}
	public void setArticle(DatasetCleaned article) {
		this.article = article;
	}
	
	public short getFrequency() {
		return frequency;
	}
	//	Generating word map for this article
	private void setFrequency() {//generate a word map for this article
		HashMap<String, Long> wordMap = article.getWordMap();
		if(wordMap.containsKey(term)) {
			this.frequency = wordMap.get(term).shortValue();
		}
		else {
		this.frequency = 0;}
	}

}
