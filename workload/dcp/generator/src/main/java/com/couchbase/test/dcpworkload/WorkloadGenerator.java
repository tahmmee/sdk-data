package com.couchbase.test.dcpworkload;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;
import com.couchbase.client.java.*;
import com.couchbase.client.java.document.*;
import com.couchbase.client.java.document.json.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.util.UUID;


public class WorkloadGenerator {
	static final Logger logger = LoggerFactory.getLogger(WorkloadGenerator.class);

	public static void main(String... args) throws Exception {
		System.out.printf("Start WorkloadGenerator!\n");
		if (args.length < 4) {
			logger.error("Missing arguments for HOST, BUCKET_NAME, WORKLOAD_SIZE");
			System.exit(1);
		}

		LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
		StatusPrinter.print(lc);

		String host = "", bucketName = "";
		int workload = 120;

		try {
			host = args[1];
			bucketName = args[2];
			workload = Integer.parseInt(args[3]);
		} catch (Exception e) {
			logger.error("Exception:" + e.toString());
			System.exit(1);
		}

		logger.info("Connecting to "+host+", bucket "+bucketName);

		boolean isConnected = false;
		CouchbaseCluster cluster = null;
		while (!isConnected) {
			try {
				logger.info("Acquire cluster " + host);
				cluster = CouchbaseCluster.create(host);
				isConnected = true;
			} catch (Exception e) {
				logger.info("Connection failed to host " + host);
				logger.info("Will retry after 10 secs\n");
				Thread.sleep(10000);
			}
		}
		if (cluster == null) {
			logger.error("Cluster is null");
			System.exit(1);
		}
		logger.info("Opening bucket " + bucketName);
		boolean isBucketAvailable = false;
		Bucket bucket = null;
		while (!isBucketAvailable) {
			try {
				bucket = cluster.openBucket(bucketName);
				isBucketAvailable = true;
			} catch (Exception e) {
				logger.info("Bucket is not available wait for 10 secs\n");
				try {
					Thread.sleep(10000);
				} catch (Exception ex) {
					logger.error("Exception: " + ex.toString());
					System.exit(1);
				}
			}
		}

	 	logger.info("Generating {} workload", workload);
		FileWriter fw = new FileWriter("/status/"+System.getenv("HOSTNAME")+"-running");
		fw.write("");
		fw.close();

		for (int i = 0; i < workload; i++) {
			try {
				bucket.upsert(JsonDocument.create("doc:"+i, JsonObject.create().put("uuid", UUID.randomUUID().toString())));
				logger.info("upsert doc:"+i);
				Thread.sleep(1000);
			} catch (Exception ex) {
				logger.error("Exception: " + ex.toString());
				System.exit(1);
			}
		}
		logger.info("Closing bucket");
		bucket.close();
		fw = new FileWriter("/status/"+System.getenv("HOSTNAME")+"-done");
		fw.write("");
		fw.close();
		cluster.disconnect();

	}
}
