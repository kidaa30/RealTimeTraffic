package storm.realTraffic;

/**
 * Copyright 2013 Xdata@SIAT
 * 
 * Last Updated:2013-1-7 ÏÂÎç8:09:20
 * 
 * email: gh.chen@siat.ac.cn
 */

import storm.realTraffic.bolt.DBWriterBolt;
import storm.realTraffic.bolt.MapMatchingBolt;
import storm.realTraffic.bolt.SpeedCalculatorBolt;
//import storm.realTraffic.bolt.TresholdCalculatorBolt;
import storm.realTraffic.spout.FieldListenerSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;



public class RealTimeTrafficTopology {
    
    public static void main(String[] args) throws AlreadyAliveException, 
                                                   InvalidTopologyException, 
                                                   InterruptedException 
 {
	FieldListenerSpout fieldListenerSpout = new FieldListenerSpout();
	
	SpeedCalculatorBolt  thresholdBolt = new SpeedCalculatorBolt ();
	MapMatchingBolt mapMatchingBolt= new MapMatchingBolt();
	SpeedCalculatorBolt speedCalculatorBolt =new SpeedCalculatorBolt();
	DBWriterBolt dbWriterBolt = new DBWriterBolt();
	
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("spout", fieldListenerSpout, 2);
        builder.setBolt("matchingBolt", mapMatchingBolt,3).shuffleGrouping("spout");        
        builder.setBolt("speedCalBolt", speedCalculatorBolt,3).shuffleGrouping("matchingBolt");
        builder.setBolt("dbWriterBolt", dbWriterBolt,1).shuffleGrouping("speedCalBolt");


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
