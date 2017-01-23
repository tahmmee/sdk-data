package com.couchbase.client.dcptest;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.state.StateFormat;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Action1;


public class BasicClient
{
	static final Logger logger = LoggerFactory.getLogger(BasicClient.class);
	static final Logger eventLogger = LoggerFactory.getLogger("event");
	public static void main( String[] args ) throws Exception
	{
		LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
		StatusPrinter.print(lc);

		if (args.length < 7) {
			logger.error("Missing arguments for SEED_NODE, BUCKET, START_VBUCKET, END_VBUCKET, PERSIST");
			System.exit(1);
		}

		String seedNode = args[2];
		String bucket = args[3];
		short start;
		short end;
		boolean persist = false;
		Short[] vbids = null;
		int vbsize = 0;

		try {
			start = (short)Integer.parseInt(args[4]);
			end = (short)Integer.parseInt(args[5]);
			persist = args[6].equalsIgnoreCase("y") ? true : false;
			vbids = new Short[end-start+1];
			vbsize = vbids.length;
			for (short i=0,  j=start; i < vbsize; i++, j++) {
				vbids[i] = j;
			}
			logger.info("start:{} end:{}", start, end);

		} catch(Exception e) {
			logger.error("Exception:" + e.toString());
			System.exit(1);
		}

		// SIGTERM handler to close log before exit
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				logger.info("Shutting down...");
				try {
					// in real world, client.disconnect().await() might not ba called when the system is rebooted
				} catch (Exception e) {
					logger.error("Exception:" + e.toString());
				}
			}
		});

		final AtomicLong cntMutation = new AtomicLong(0);
		final AtomicLong cntControl = new AtomicLong(0);

		logger.info("seedNode={}, bucket={}", seedNode, bucket);
		com.couchbase.client.dcp.Client client;
		client = com.couchbase.client.dcp.Client.configure()
				.hostnames(seedNode)
				.bucket(bucket)
				.build();


		client.controlEventHandler(new ControlEventHandler() {
			public void onEvent(ByteBuf event) {
				eventLogger.info("CTRL:"+event.toString());
				cntControl.addAndGet(1);
				event.release();
			}
		});
		client.dataEventHandler(new DataEventHandler() {
			public void onEvent(ByteBuf event) {
				cntMutation.addAndGet(1);
				if (DcpMutationMessage.is(event)) {
					String key = DcpMutationMessage.key(event).toString(CharsetUtil.UTF_8);
					String content = DcpMutationMessage.content(event).toString(CharsetUtil.UTF_8);
					logger.info("MUT:key={}, content={}", key, content);
					eventLogger.info("MUT:key={}, content={}", key, content);
				} else if (DcpDeletionMessage.is(event)) {
					String key = DcpDeletionMessage.key(event).toString(CharsetUtil.UTF_8);
					logger.info("DEL:key={}", key);
					eventLogger.info("DEL:key={}", key);
				} else {
					logger.error("Unknown event={}", event);
				}
				event.release();
			}
		});

		logger.info("*************  load the persisted state from file if it exists *************");
		String FILENAME = "/work/"+System.getenv("HOSTNAME")+"/state-" + bucket + ".json";
		File file = new File(FILENAME);
		byte[] persisted = null;
		logger.info("************* Connecting **********");
		client.connect().await();
		logger.info("************* Connected  **********");
		if (file.exists() && persist) {
			try {
				String sessionStatePath = FILENAME;
				final ObjectMapper JACKSON = new ObjectMapper();
					try {
						File sessionStateFile = new File(sessionStatePath);
						logger.info("Validate session");
						SessionState sessionState = JACKSON.readValue(sessionStateFile, SessionState.class);
						final AtomicInteger idx = new AtomicInteger(0);
						sessionState.foreachPartition(new Action1<PartitionState>() {
							public void call(PartitionState state) {
								int partition = idx.getAndIncrement();
								logger.info("reading partition {}, startseq:{}, endseq:{}, snapshotstartseq:{}, snapshotendseq:{}", partition, state.getStartSeqno(), state.getEndSeqno(), state.getSnapshotStartSeqno(), state.getSnapshotEndSeqno());
								if (lessThan(state.getEndSeqno(), state.getStartSeqno())) {
									logger.info("stream request for partition %d will fail because " +
													"start sequence number (%d) is larger than " +
													"end sequence number (%d)\n",
											partition, state.getStartSeqno(), state.getEndSeqno());
								}
								if (lessThan(state.getStartSeqno(), state.getSnapshotStartSeqno())) {
									logger.info("stream request for partition %d will fail because " +
													"snapshot start sequence number (%d) must not be larger than " +
													"start sequence number (%d)\n",
											partition, state.getSnapshotStartSeqno(), state.getStartSeqno());
								}
								if (lessThan(state.getSnapshotEndSeqno(), state.getStartSeqno())) {
									logger.info("stream request for partition %d will fail because " +
													"start sequence number (%d) must not be larger than " +
													"snapshot end sequence number (%d)\n",
											partition, state.getStartSeqno(), state.getSnapshotEndSeqno());
								}
							}
						});
						logger.info("Done Validate session");
					} catch (IOException e) {
						logger.error("Failed to decode " + sessionStatePath + ": " + e);
					}

				FileInputStream fis = new FileInputStream(FILENAME);
				persisted = IOUtils.toByteArray(fis);
				fis.close();
			} catch (Exception e) {
				logger.error("Exception:" + e.toString());
				System.exit(1);
			}
			logger.info("*************  start from the last saved state *************");
			client.recoverState(StateFormat.JSON, persisted);
		} else {
			logger.info("*************  start from now *************");
			client.initializeState(StreamFrom.NOW, StreamTo.INFINITY).await();
		}
		if (vbsize < 1024) {
			client.startStreaming(vbids).await();
		} else {
			client.startStreaming().await();
		}

		FileWriter fw = new FileWriter("/status/"+System.getenv("HOSTNAME")+"-running");
		fw.write("");
		fw.close();

		while(true) {
			try {
				Thread.sleep(TimeUnit.SECONDS.toMillis(10));

				byte[] state = client.sessionState().export(StateFormat.JSON);

				FileOutputStream output = new FileOutputStream(new File(FILENAME));
				IOUtils.write(state, output);
				output.close();
				logger.info(System.currentTimeMillis() + " - Persisted State to {}", FILENAME);
			} catch (Exception e){
				logger.error("Exception:" + e.toString());
			}
		}
	}

	private static boolean lessThan(long x, long y) {
		return (x < y) ^ (x < 0) ^ (y < 0);
	}

}
