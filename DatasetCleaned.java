package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;


public class DatasetCleaned implements Serializable{
	private static final long serialVersionUID = 7860293794078492243L;
	
	String id;  // Umique Article ID
	List<String> title;   // Article Title
	List<String> paragraph; // Paragraph contents 
	Long doc_length;  // Length of the article
	HashMap<String, Long> wordMap = new HashMap<String, Long>(); // Mapping to record the Word-Frequency Pair
	NewsArticle originalArticle;  // Fetch the original object
	
	public DatasetCleaned(String id, List<String> title, List<String> paragraph, Long doc_length, NewsArticle originalArticle) {
		super();
		this.id = id;
		this.title = title;
		this.paragraph = paragraph;
		this.doc_length = doc_length;
		WordFrequency();
		this.originalArticle = originalArticle;
	}
	

	public DatasetCleaned() {
		// TODO Auto-generated constructor stub
	}


	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<String> getTitle() {
		return title;
	}

	public void setTitle(List<String> title) {
		this.title = title;
	}

	public List<String> getParagraph() {
		return paragraph;
	}

	public void setParagraph(List<String> paragraph) {
		this.paragraph = paragraph;
	}

	public Long getDoc_length() {
		return doc_length;
	}

	public void setDoc_length(Long doc_length) {
		this.doc_length = doc_length;
	}

	public NewsArticle getOriginalArticle() {
		return originalArticle;
	}

	public void setOriginalArticle(NewsArticle originalArticle) {
		this.originalArticle = originalArticle;
	}
	
	
	public HashMap<String, Long> getWordMap() {
		return wordMap;
	}


	public void setWordMap(HashMap<String, Long> wordMap) {
		this.wordMap = wordMap;
	}


	public void WordFrequency() {
		List<String> content = new ArrayList<String>();
		if(this.title != null) content.addAll(this.title);
		
		// The "content" is contents of the title and the paragraph
		if(content != null) content.addAll(this.paragraph);  
		
		// Fetching the wordMap for this article
		for (String word : content) {	
            if (this.wordMap.containsKey(word)) {
                wordMap.put(word, wordMap.get(word) + (long)1);
            } else {
                wordMap.put(word, (long) 1);
            }
       	}
	}
	
}
