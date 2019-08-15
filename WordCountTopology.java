package com.upgrad.wordcount;

/*
 * This is the main driver class where topology with spout and bolts is built, different parameters are configured and then topology is submitted.
 * It has the option of submitting the topology both in local and production mode.
 */
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.StormSubmitter;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class WordCountTopology {

	private static final String SENTENCE_SPOUT_ID = "SentenceSpout";
	private static final String SPLIT_BOLT_ID = "SplitSentenceBolt";
	private static final String COUNT_BOLT_ID = "WordCountBolt";
	private static final String REPORT_BOLT_ID = "ReportBolt";
	private static final String TOPOLOGY_NAME = "word-count-topology";

	public static void main(String[] args) throws Exception {

		SentenceSpout spout = new SentenceSpout();
		SplitSentenceBolt splitBolt = new SplitSentenceBolt();
		WordCountBolt countBolt = new WordCountBolt();
		ReportBolt reportBolt = new ReportBolt();

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout(SENTENCE_SPOUT_ID, spout, 1);
		// SentenceSpout --> SplitSentenceBolt
		builder.setBolt(SPLIT_BOLT_ID, splitBolt, 3).shuffleGrouping(SENTENCE_SPOUT_ID);
		// SplitSentenceBolt --> WordCountBolt
		/*
		 * Using shuffleGrouping for load balancing - Tuples are randomly distributed
		 * across the bolts in a way such that each bolt is guaranteed to get an equal
		 * number of tuples.
		 */
		builder.setBolt(COUNT_BOLT_ID, countBolt, 4).shuffleGrouping(SPLIT_BOLT_ID);
		// WordCountBolt --> ReportBolt
		builder.setBolt(REPORT_BOLT_ID, reportBolt, 1).globalGrouping(COUNT_BOLT_ID);

		Config config = new Config();

		if (args != null && args.length > 0) {
			/*
			 * Setting number of workers to 3
			 */
			config.setNumWorkers(3);
			/*
			 * Setting number of event logger executors to 3, equal to number of workers
			 */
			config.setNumEventLoggers(3);

			StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
		} else {

			LocalCluster cluster = new LocalCluster();
			/*
			 * Removed hard coding of topology name from here and used a static variable
			 * defined at the beginning (TOPOLOGY_NAME)
			 */
			cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
			Utils.sleep(100000);
			/*
			 * Removed hard coding of topology name from here and used a static variable
			 * defined at the beginning (TOPOLOGY_NAME)
			 */
			cluster.killTopology(TOPOLOGY_NAME);
			cluster.shutdown();
		}

	}
}