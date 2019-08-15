package com.upgrad.wordcount;

/*
 * This spout (SentenceSpout) emits sentences as tuples in the topology which are processed by various bolts.
 * Reliability API has been implemented along with replay mechanism for failed tuples. This spout will associate an
 * unique message ID to every emitted tuple.
 */

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class SentenceSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private String[] sentences = { 
			"my dog has fleas",
			"i like cold beverages",
			"the dog ate my homework",
			"dont have a cow man", 
			"i dont think i like fleas"
			};
	private int index = 0;
	/*
	 * Declaring 2 Maps for replay mechanisms for failed tuples 1. toSend - will
	 * store failed tuples and will be used to replay failed tuples 2. messages -
	 * will store emitted tuples from spout
	 */
	private Map<UUID, String> toSend = null;
	private Map<UUID, String> messages = null;

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

	public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {

		this.collector = collector;
		/*
		 * Creating 2 Maps for replay mechanisms for failed tuples
		 */
		this.toSend = new HashMap<UUID, String>();
		this.messages = new HashMap<UUID, String>();
	}

	public void nextTuple() {
		/*
		 * Checking if toSend contains any failed tuples and if so then get all entries
		 * from collection and emit (replay) again and display message on which msgID
		 * being replayed. Once all failed tuples stored in toSend are replayed, toSend
		 * can be cleared safely.
		 */

		if (!toSend.isEmpty()) {
			for (Map.Entry<UUID, String> sentenceEntry : toSend.entrySet()) {
				UUID sentenceId = sentenceEntry.getKey();
				String sentenceMessage = sentenceEntry.getValue();
				System.out.println("Replaying Failed Message ID: " + sentenceId);
				collector.emit(new Values(sentenceMessage), sentenceId);
			}
			toSend.clear();
		}
		/*
		 * Setting up msgID object to contain random unique id generated in order to
		 * associate messageID to the tuple.
		 */
		UUID msgID = UUID.randomUUID();
		/*
		 * Second argument to the emit method is a msgID generated using java.util.UUID
		 * library. In order to use reliability API to ensure each tuple is processed at
		 * least once, associating a unique messageID to each tuple emitted by Spout.
		 */
		this.collector.emit(new Values(sentences[index]), msgID);
		/*
		 * Adding emitted tuple to messages map.
		 */
		messages.put(msgID, sentences[index]);

		index++;
		if (index >= sentences.length) {
			index = 0;
		}

		/*
		 * Adding a sleep of 1 millisecond just to avoid overloading storm topology.
		 */
		Utils.sleep(1);
	}

	/*
	 * Adding implementation for ack() method to print acknowledged message ID and
	 * remove it from messages map which contains all emitted tuples.
	 */
	public void ack(Object msgId) {
		System.out.println("Acked Message ID : " + msgId);
		UUID sentenceId = (UUID) msgId;
		messages.remove(sentenceId);
	}

	/*
	 * Adding implementation for fail() method to print Failed message ID and add
	 * failed tuple in toSend map so can be replayed.
	 */
	public void fail(Object msgId) {
		System.out.println("Failed Message ID : " + msgId);
		UUID sentenceId = (UUID) msgId;
		toSend.put(sentenceId, messages.get(sentenceId));
	}

}