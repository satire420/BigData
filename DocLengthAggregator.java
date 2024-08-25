package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.ReduceFunction;

/**
 * Determining aggregate document length
 */

public class DocLengthAggregator implements ReduceFunction<Long> {
	
	private static final long serialVersionUID = 5890430376953697462L;
	
	@Override
	
	// This function applies a pairwise aggregation operation repeatedly on a list, ultimately yielding a single value.
	public Long call(Long v1, Long v2) throws Exception {
		return v1+v2;
	}	
}
