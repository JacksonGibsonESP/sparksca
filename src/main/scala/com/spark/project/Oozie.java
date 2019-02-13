package com.spark.project;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;

import java.util.Properties;

public class Oozie {
    public static void main(String[] args) throws OozieClientException, InterruptedException {
        // get a OozieClient for local Oozie
        OozieClient wc = new OozieClient("http://quickstart.cloudera:11000/oozie/");

        // create a workflow job configuration and set the workflow application path
        Properties conf = wc.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, "hdfs://quickstart.cloudera:8020/user/hue/oozie/workspaces/hue-oozie-1549975202.55");

        // setting workflow parameters
        conf.setProperty("jobTracker", "quickstart.cloudera:8032");
        conf.setProperty("nameNode", "hdfs://quickstart.cloudera:8020");

        // submit and start the workflow job
        String jobId = wc.run(conf);
        System.out.println("Workflow job submitted");

        // wait until the workflow job finishes printing the status every 10 seconds
        while (wc.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING) {
            System.out.println("Workflow job running ...");
            Thread.sleep(10 * 1000);
        }

        // print the final status of the workflow job
        System.out.println("Workflow job completed ...");
        System.out.println(wc.getJobInfo(jobId));
    }
}
