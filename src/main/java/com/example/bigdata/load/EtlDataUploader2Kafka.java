package com.example.bigdata.load;

import com.example.bigdata.processor.Hdfs2Kafka;

public class EtlDataUploader2Kafka {
    public static void main(String[] args) throws Exception{
        Hdfs2Kafka hdfs2Kafka = new Hdfs2Kafka();

        hdfs2Kafka.readHdFile("unemployee_annual.csv")
                .forEach(str -> hdfs2Kafka.sendLines2Kafka("topic_unempl_ann", str));
        hdfs2Kafka.getHdFilesInfo("unemployee_annual.csv");

        hdfs2Kafka.closeStream();
    }
}
