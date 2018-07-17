package com.imooc.bigdata;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
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
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * 使用storm实现累计求和
 */
public class ClusterSumStormTopology {

    /**
     * 数据源需要产生数据并发射
     */
    public static class DataSourceSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;

        /**
         * 初始化数据，只会调用一次
         * @param map
         * @param topologyContext
         * @param collector
         */
        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {

            this.collector = collector;

        }

        int number = 0;

        /**
         * 会产生数据，在生产上肯定是从消息队列中获取数据,
         * 是一个死循环，会一直不停地执行
         *
         */
        @Override
        public void nextTuple() {

            collector.emit(new Values(++number));

            System.out.println("Spout: " + number);

            Utils.sleep(1000);

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

            declarer.declare(new Fields("num"));

        }
    }

    /**
     * 数据累计求和
     */
    public static class SumBolt extends BaseRichBolt {

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {

        }

        int sum = 0;

        @Override
        public void execute(Tuple input) {

            Integer num = input.getIntegerByField("num");

            sum += num;

            System.out.println("Bolt: sum = [" + sum  + "]");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        builder.setBolt("SumBolt", new SumBolt()).shuffleGrouping("DataSourceSpout");

        String topoName = ClusterSumStormTopology.class.getSimpleName();
        try {
            StormSubmitter.submitTopology(topoName, new Config(), builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
