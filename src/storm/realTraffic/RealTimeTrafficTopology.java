package storm.realTraffic;

import storm.realTraffic.bolt.SpeedCalculatorBolt;
//import storm.realTraffic.bolt.TresholdCalculatorBolt;
import storm.realTraffic.spout.FieldListenerSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
//import backtype.storm.task.ShellBolt;
//import backtype.storm.task.TopologyContext;
//import backtype.storm.topology.BasicOutputCollector;
//import backtype.storm.topology.IRichBolt;
//import backtype.storm.topology.OutputFieldsDeclarer;
//import backtype.storm.topology.TopologyBuilder;
//import backtype.storm.topology.base.BaseBasicBolt;
//import backtype.storm.tuple.Fields;
//import backtype.storm.tuple.Tuple;
//import backtype.storm.tuple.Values;
//import java.util.HashMap;
//import java.util.Map;


/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class RealTimeTrafficTopology {
    
    public static void main(String[] args) throws AlreadyAliveException, 
                                                   InvalidTopologyException, 
                                                   InterruptedException 
 {
	FieldListenerSpout fieldListenerSpout = new FieldListenerSpout();
	SpeedCalculatorBolt  thresholdBolt = new SpeedCalculatorBolt ();
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("spout", fieldListenerSpout, 2);
        
        builder.setBolt("thresholdBolt", thresholdBolt,3).shuffleGrouping("spout");
        //builder.setBolt("dbWriterBolt",dbWriterBolt,1).shuffleGrouping("thresholdBolt");


	    Config conf = new Config();
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);            
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } 
        else {     
              
              conf.setDebug(true);
              conf.setMaxTaskParallelism(3);
              LocalCluster cluster = new LocalCluster();
              cluster.submitTopology(
              "Threshold_Test", conf, builder.createTopology());
    	      Thread.sleep(9000);
    	      cluster.shutdown(); 
        }

    }
}
