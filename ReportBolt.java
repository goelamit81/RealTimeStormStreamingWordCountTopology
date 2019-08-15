package com.upgrad.wordcount;

/*
 * This (ReportBolt) is the last bolt in the topology. Aggregation is happening in this bolt.
 * This bolt is receiving tuples from previous bolt (WordCountBolt) and computing counts for words
 * and logging (word and count) in the database table. It will acknowledge processed tuples. 
 * Acknowledging is controlled via a boolean so just in case, we want to test if replay of failed 
 * tuples works fine. If this boolean is true then processed tuples will be acknowledged and 
 * if this boolean is false then tuples will be failed so will get replayed by spout.
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.sql.*;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class ReportBolt extends BaseRichBolt {

	private OutputCollector collector;
	private HashMap<String, Long> ReportCounts = null;
	private int temp_count_variable = 0;
	private Connection con;
	private Statement stmt;
	private static final String username = "root";
	private static final String password = "cloudera";
	/*
	 * Setting up a constant to acknowledge or fail a tuple. If set it true, all
	 * tuples will be acknowledged. If set to false, all tuples will be failed and
	 * will then be replayed by spout. This was just done to setup a way to test
	 * replay of failed tuples.
	 */
	private static final boolean acking = true;
	/*
	 * Setting up a constant in order to update database, to avoid frequent updates.
	 * Database will be updated after every cnt_tuples_update number of tuples are
	 * processed. This will help in reducing latency. Reducing this constant causes
	 * increase in complete latency.
	 */
	private static final int cnt_tuples_update = 100000;

	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.ReportCounts = new HashMap<String, Long>();
		// code added to make connection to mysql database.
		try {
			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection("jdbc:mysql://localhost:3306/upgrad", username, password);
			stmt = con.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("word");
		/*
		 * As using shuffleGrouping instead of fieldGrouping for WordCountBolt, same
		 * word can be sent to different instances of WordCountBolt and so ReportBolt
		 * can receive same word from different instances of WordCountBolt and so
		 * counting for the word needs to be done appropriately. As globalGrouping being
		 * used for ReportBolt so all instances of WordCountBolt will send tuples to
		 * same instance of ReportBolt. Checking first if word is present in HashMap
		 * already, if present, increment its existing count with 1 and if not then set
		 * current count as 1 for the word and adding in ReportCounts map.
		 */
		Long count = this.ReportCounts.get(word);
		if (count == null) {
			count = 0L;
		}
		count++;
		this.ReportCounts.put(word, count);

		temp_count_variable++;
		/*
		 * Trying not to update table too frequently so when cnt_tuples_update number of
		 * tuples are processed then update the table and reset the temp_count_variable
		 * back to 0. If cnt_tuples_update is reduced, it will increase complete
		 * latency.
		 */
		if (temp_count_variable == cnt_tuples_update) {
			temp_count_variable = 0;
			List<String> keys = new ArrayList<String>();
			keys.addAll(this.ReportCounts.keySet());
			Collections.sort(keys);
			for (String key : keys) {
				/*
				 * Setting up a kind of merge statement so when word does not exist in table
				 * already, data (word and count) will be inserted and if word is already
				 * present in table then its count will be updated. Field "word" is primary key
				 * in the table.
				 */
				String InsertQuery = "INSERT INTO wordcounts(word,count) VALUES('" + key + "',"
						+ this.ReportCounts.get(key) + ") ON DUPLICATE KEY UPDATE count=" + this.ReportCounts.get(key);

				try {
					System.out.println("Database Updating");
					boolean rs = stmt.execute(InsertQuery);
					System.out.println("Database Updated");
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}

		}
		/*
		 * Setting up acknowledging or failing of tuple based on a boolean. By default,
		 * this boolean is set to true and so every processed tuple will be
		 * acknowledged. Just in case, we want to test if replay of failed tuples is
		 * working fine, we can change this boolean to false and then every tuple will
		 * start failing and then will be replayed by SentenceSpout.
		 */
		if (acking) {
			this.collector.ack(tuple);
		} else {
			this.collector.fail(tuple);
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// this bolt does not emit anything

	}

	public void cleanup() {
		System.out.println("--- FINAL COUNTS ---");
		List<String> keys = new ArrayList<String>();
		keys.addAll(this.ReportCounts.keySet());
		Collections.sort(keys);
		for (String key : keys) {
			System.out.println(key + " : " + this.ReportCounts.get(key));
		}
		System.out.println("--------------");
	}
}