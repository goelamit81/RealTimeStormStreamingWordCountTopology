package com.upgrad.wordcount;

/*
 * This bolt (WordCountBolt) is receiving tuples from previous bolt (SplitSentenceBolt) and just passing it to next bolt 
 * (ReportBolt) without any processing. As we are using shuffle grouping, all instances of this bolt 
 * (WordCountBolt) will receive equal number of tuples. In order to achieve load balancing, i am using 
 * shuffleGrouping for this bolt and as a result, there is no need to perform any processing in this bolt. 
 * This bolt can just pass the tuple to next bolt. Aggregation will be done in next bolt (ReportBolt). 
 * Reliability API has been implemented. This bolt will anchor the tuple and will acknowledge the processed tuple.
 */
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordCountBolt extends BaseRichBolt {

	private OutputCollector collector;

	/*
	 * HashMap counts was being used in code provided. I have removed it as not
	 * needed for assignment.
	 */
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("word");
		/*
		 * First argument to the emit method is tuple itself. In order to use
		 * reliability API to ensure each tuple is processed at least once, anchoring
		 * tuple.
		 */
		this.collector.emit(tuple, new Values(word));
		/*
		 * In order to use reliability API to ensure each tuple is processed at least
		 * once, acknowledging when tuple is processed.
		 */
		this.collector.ack(tuple);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
}