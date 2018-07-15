package com.imooc.bigdata;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class LocalWordCountTopology {


    public static class DataSourceSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;

        @Override
        public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {

            this.collector = collector;
        }

        /**
         * 读取/Users/cancan/Documents/storm-train
         */
        @Override
        public void nextTuple() {
            Collection<File> files = FileUtils.listFiles(new File("/Users/cancan/Documents/storm-train"), new String[]{"txt"}, true);
            for (File f : files) {
                try {
                    List<String> lines = FileUtils.readLines(f);
                    for (String line : lines) {
                        collector.emit(new Values(line));
                    }
                    //数据处理完，改名，否则一直读取
                    FileUtils.moveFile(f, new File(f.getAbsolutePath() + System.currentTimeMillis()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("line"));
        }
    }

    public static class SplitBolt extends BaseRichBolt {

        private OutputCollector collector;

        @Override
        public void prepare(Map map, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {

            String line = input.getStringByField("line");
            String[] words = line.split(" ");
            for (String word : words) {
                this.collector.emit(new Values(word));
            }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static class CountBolt extends BaseRichBolt {

        @Override
        public void prepare(Map map, TopologyContext context, OutputCollector collector) {

        }

        Map<String, Integer> map = new HashMap<>();

        @Override
        public void execute(Tuple input) {

            String word = input.getStringByField("word");

            Integer count = map.get(word);

            if (count == null) {
                count = 1;
            } else {
                count++;
            }

            map.put(word, count);

            Set<String> keys = map.keySet();

            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

            for(String key : keys) {
                System.out.println(key + ":" + map.get(key));
            }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {


        }
    }

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        builder.setBolt("SplitBolt", new SplitBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("CountBolt", new CountBolt()).shuffleGrouping("SplitBolt");


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalWordCountTopology", new Config(), builder.createTopology());
    }
}
