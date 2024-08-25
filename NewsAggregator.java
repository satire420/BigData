package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.List;
import java.util.ArrayList;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.DatasetCleaned;


public class NewsAggregator implements MapFunction<NewsArticle, DatasetCleaned>{
	private static final long serialVersionUID = -4631167868446468000L;

	private transient TextPreProcessor newsProcessor;
	
	public NewsAggregator() {
		
	}
	
	@Override
	public DatasetCleaned call(NewsArticle value) throws Exception {
		
		List<String> title = new ArrayList<String>();
		List<String> terms = new ArrayList<String>();
		Long doc_length = (long) 0;

		if (newsProcessor==null) newsProcessor = new TextPreProcessor();
		
		//	Initialize
		String newsID = value.getId();
		String newsTitle = value.getTitle();
		List<ContentItem> newsContentItems = value.getContents();
		String newsParagraph = "";

		int i = 0;
		
		// Iterate through the contentItems
		for(ContentItem newsContentItem : newsContentItems) {
			// If newsContentsItem is not null ,so we can get subType
			if(newsContentItem!=null) { 	
				String subType = newsContentItem.getSubtype();

			if( subType!= null) {
				if (subType.equals("paragraph")){
					// If the paragraph is null or blank, -> skip to the next paragraph
					if(!newsContentItem.getContent().equals(null) || !newsContentItem.getContent().equals("")){
						newsParagraph = newsParagraph + newsContentItem.getContent();
						i++;	
					}	
				}

				if(i==5) {
					break;	//	if the count of paragraph is 5, -> stop the loop.
				}
			}
		}
	}
		if(newsParagraph!=null) {
			// Those terms are the paragraphs that have undergone text pre-processing
			terms.addAll(newsProcessor.process(newsParagraph));		
			doc_length += terms.size();
		}

		if(newsTitle != null) {
			// Title is the title after text pre-processing
			title.addAll(newsProcessor.process(newsTitle));		
			doc_length += title.size();
		}
		DatasetCleaned article =  new DatasetCleaned(newsID, title, terms, doc_length, value);;

		return article;
	}

}
