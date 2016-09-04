package com.otterinasuit.spark.datagenerator;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.random.RandomRDDs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Generates random records for this article
 * https://otter-in-a-suit.com/blog/?p=12
 *
 * @author Christian Hollinger (christian.hollinger@otter-in-a-suit.com)
 */
public class Main {
    private final static Logger logger = LoggerFactory.getLogger(Main.class);
    private final static byte[] COLUMN_FAMILY = Bytes.toBytes("cf");
    private final static byte[] USER = Bytes.toBytes("user");
    private final static byte[] ACTION = Bytes.toBytes("action");
    private final static byte[] TS = Bytes.toBytes("ts");
    private final static byte[] CLIENT = Bytes.toBytes("client");
    private final static int SPLIT = 10000;


    public static void main(String[] args) {
        long limit = (args.length > 0 && StringUtils.isNumeric(args[0])) ? Long.parseLong(args[0]) : 2000000;
        int threads = (args.length > 1 && StringUtils.isNumeric(args[1])) ? Integer.parseInt(args[1]) : 4;
        SparkConf conf = new SparkConf().setAppName("com.otter-in-a-suit.CreateTestHBaseRecords")
                .setMaster("local["+threads+"]");
        JavaSparkContext context = new JavaSparkContext(conf);

        // mllib to the rescue
        JavaDoubleRDD rdd1 = RandomRDDs.normalJavaRDD(context, limit);
        JavaRDD<Action> actionRDD = rdd1.map(e -> new Action());

        // Disable debug logging!
        actionRDD.foreach((VoidFunction<Action>) action -> logger.debug(action.toString()));

        // Put into HBase
        JavaRDD<Action> result = actionRDD.mapPartitions((FlatMapFunction<Iterator<Action>, Action>) data -> {
            try {
                logger.info("mapPartitions");
                Configuration config = new Configuration();
                Connection connection = ConnectionFactory.createConnection(config);
                Table table = connection.getTable(TableName.valueOf("action"));
                while (data.hasNext()) {
                    Action action = data.next();

                    Put put = new Put(Bytes.toBytes(action.getKey()));
                    put.addColumn(COLUMN_FAMILY, ACTION, Bytes.toBytes(action.getAction()));
                    put.addColumn(COLUMN_FAMILY, USER, Bytes.toBytes(action.getUser()));
                    put.addColumn(COLUMN_FAMILY, TS, Bytes.toBytes(action.getTs()));
                    put.addColumn(COLUMN_FAMILY, CLIENT, Bytes.toBytes(action.getClient()));

                    table.put(put);
                }
                // Cleanup
                table.close();
                connection.close();
            } catch (Exception e) {
                logger.error("Writing to HBase failed!");
                e.printStackTrace();
            }
            return data;
        });


        result.foreach((VoidFunction<Action>) action -> {
            // TODO: JavaHBaseContext
        });
    }

}