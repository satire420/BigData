package uk.ac.gla.dcs.bigdata.studentstructures;

import org.apache.spark.util.AccumulatorV2;
import java.util.HashMap;
import java.util.Map;

/**
 * Prakhar Lad modified it based on AccumulatorV2
 */

public class TermFrequencyAcc extends AccumulatorV2<Map<String, Integer>, Map<String, Integer>> {
	private static final long serialVersionUID = 1123123123123L;
	
	private Map<String, Integer> map = new HashMap<>();

	public TermFrequencyAcc() {
		// TODO Auto-generated constructor stub
	}
	
	public TermFrequencyAcc(Map<String, Integer> map) {
        this.map = map;
    }
	
	@Override
    public boolean isZero() {
        return map.isEmpty();
    }
	
	@Override
    public AccumulatorV2<Map<String, Integer>, Map<String, Integer>> copy() {
        return new TermFrequencyAcc(new HashMap<>(map));
    }
	
	@Override
    public void reset() {
        map.clear();
    }
	
	@Override
    public void add(Map<String, Integer> values) {
        for (Map.Entry<String, Integer> entry : values.entrySet()) {
            if (map.containsKey(entry.getKey())) {
                map.put(entry.getKey(), map.get(entry.getKey()) + entry.getValue());
            } else {
                map.put(entry.getKey(), entry.getValue());
            }
        }
    }
	
	@Override
    public void merge(AccumulatorV2<Map<String, Integer>, Map<String, Integer>> other) {
        Map<String, Integer> otherMap = other.value();
        for (Map.Entry<String, Integer> entry : otherMap.entrySet()) {
            if (map.containsKey(entry.getKey())) {
                map.put(entry.getKey(), map.get(entry.getKey()) + entry.getValue());
            } else {
                map.put(entry.getKey(), entry.getValue());
            }
        }
    }
	
	@Override
    public Map<String, Integer> value() {
        return map;
    }
	
	

}
