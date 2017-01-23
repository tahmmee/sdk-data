package com.couchbase.client.dcptest;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App
{
    static final Logger logger = LoggerFactory.getLogger(App.class);
    static final ClientConfig config = new DefaultClientConfig();
    static final Client restClient = Client.create(config);
    static final String id = "Administrator";
    static final String password = "password";
    static Cluster cluster;
    static final Compose compose = new Compose();
    public static void main( String[] args ) throws Exception
    {
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        StatusPrinter.print(lc);

        if (!setup()) {
            logger.error("Error while setting up");
            lc.stop();
            System.exit(1);
        }
        logger.info("*************  TEST CASE : Remove nodes  *************");
        String seed = "couchbase-a-0";
        String bucket = "default";
        String persist = "n";
        int clientNum = 1;
        int start = 0;
        int size = (1024/clientNum);
        int end = size-1;

        for (int i = 0; i < clientNum ; i++) {
            File status = new File("/status/dcp-client-"+Integer.toString(i)+"-ready");
            while (!status.exists()) {
                try {
                    logger.info("Waiting for dcp-client-"+Integer.toString(i)+" ready");
                    Thread.sleep(2000);
                } catch (Exception e) {
                    logger.error("Exception:{}", e.toString());
                    System.exit(1);
                }
            }
            if (!compose.Execute("dcp-client-"+Integer.toString(i), "/start.sh", "basic com.couchbase.client.dcptest.BasicClient "+seed+" "+bucket+" "+Integer.toString(start)+" "+Integer.toString(end)+" "+persist)) {
                logger.error("Failed to start dcp-client-"+Integer.toString(i));
                System.exit(1);
            }
            start += size;
            end += size;
            end = (end > 1023) ? 1023 : end;
        }

        try {
            Thread.sleep(10000);
        } catch (Exception e) {
        }

        logger.info("*************  Generate 120 mutations *************");
        int mutationNo = 120;

        if (!compose.Execute("workload-0", "/start.sh", "dcp/generator couchbase-a-0 default "+Integer.toString(mutationNo))) {
            logger.error("Failed starting workload generator");
            System.exit(1);
        }

        File workload = new File("/status/workload-0-running");
        while (!workload.exists()) {
            try {
                logger.info("Waiting for workoad-0-running");
                Thread.sleep(2000);
            } catch (Exception e) {
                logger.error("Exception:{}", e.toString());
                System.exit(1);
            }
        }


        logger.info("Ramp for 60 seconds");
        try {
            Thread.sleep(60000);
        } catch (Exception e) {
            logger.error("Exception:{}", e.toString());
        }

        logger.info("*************  Remove a ept node by rebalancing *************");
        logger.info("Rebalancing..");
        List<String> ejectedNodes = new ArrayList<String>();
        ejectedNodes.add("ns_1@" + cluster.Addr("couchbase-a-0"));

        if (!cluster.Rebalance(ejectedNodes)) {
            logger.error("Failure while rebalancing");
            System.exit(1);
        }
        logger.info("Rebalancing finished");

        File status = new File("/status/workload-0-done");
        while(!status.exists()) {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                logger.error("Exception:{}", e.toString());
                System.exit(1);
            }
        }

        logger.info("Workoad finished. Give 10 seconds to flush DCP events");
        try {
            Thread.sleep(10000);
        } catch (Exception e) {
            logger.error("Exception:{}", e.toString());
        }


        BufferedReader br = null;
        int totalMutation = 0;
        String sCurrentLine;
        try {
            for (int i = 0; i < clientNum; i++) {
                br = new BufferedReader(new FileReader("/work/dcp-client-" + Integer.toString(i) + "/log/event"));
                while ((sCurrentLine = br.readLine()) != null) {
                    if (sCurrentLine.contains("MUT:")) {
                        totalMutation++;
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("Exception:{}", ex.toString());
        }


        FileWriter fw = new FileWriter("/work/"+System.getenv("HOSTNAME")+"/exitcode");
        if (mutationNo == (long)totalMutation) {
            fw.write("0");
            logger.info("****** TEST PASSED ******");
        } else {
            fw.write("1");
            logger.info("****** TEST FAILED ******");
            logger.info("Expected: {} mutations", mutationNo);
            logger.info("Actual  : {} mutations", totalMutation);
        }
        fw.close();
        System.out.flush();
    }

    static boolean setup() {


        cluster = new Cluster(compose, restClient, "couchbase-a", id, password);
        if (cluster == null) {
            logger.error("Can not create a couchbase cluster");
            return false;
        }

        if (cluster.Provision(5) == false) {
            logger.error("Can not provision a cluster");
            return false;
        }

        // add 3 nodes
        logger.info("Adding 3 nodes");
        if (!cluster.Add("couchbase-a-1") || !cluster.Add("couchbase-a-2") || !cluster.Add("couchbase-a-3")) {
            logger.error("Can not add nodes");
            return false;
        }
        logger.info("Added 3 nodes");

        // rebalance
        logger.info("Rebalancing..");
        List<String> ejectedNodes = new ArrayList<String>();
        if (!cluster.Rebalance(ejectedNodes)) {
            logger.error("Failure while rebalancing");
            return false;
        }
        logger.info("Rebalancing finished");

        // memory_optimized
        logger.info("Set storageMode");
        if (!cluster.SetStorageMode("memory_optimized")) {
            logger.error("Failed in setting storageMode");
            return false;
        }

        // create default bucket
        logger.info("Create default bucket");
        if (!cluster.CreateBucket("default")) {
            logger.error("Failed in creating default bucket");
            return false;
        }

        return true;
    }
}

