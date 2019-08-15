package com.upgrad.wordcount;

/*
 * This bolt (SplitSentenceBolt) is the first bolt which receives tuples emitted from spout. 
 * It splits the sentence into words and send as tuples to next bolt (WordCountBolt) in the topology. 
 * As we are using shuffle grouping, all instances of this bolt (SplitSentenceBolt) will receive equal 
 * number of tuples. Reliability API has been implemented. This bolt will anchor the tuple and will
 * acknowledge the processed tuple.
 */

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitSentenceBolt extends BaseRichBolt {

	private OutputCollector collector;

	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple tuple) {
		String sentence = tuple.getStringByField("sentence");
		String[] words = sentence.split(" ");
		for (String word : words) {
			/*
			 * First argument to the emit method is tuple itself. In order to use
			 * reliability API to ensure each tuple is processed at least once, anchoring
			 * tuple.
			 */
			this.collector.emit(tuple, new Values(word));
		}
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