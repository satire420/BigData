package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.DatasetCleaned;

/*
 * Primary function of this class is to extract the document lengths.
 */

public class DocLenExtractor implements MapFunction<DatasetCleaned, Long> {
	
	private static final long serialVersionUID = -6249869268110115686L;
	
	@Override
	
	public Long call(DatasetCleaned value) throws Exception {
		return value.getDoc_length();
	}
}
