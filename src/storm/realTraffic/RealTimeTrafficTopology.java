package storm.realTraffic;

/**
 * Copyright 2013 Xdata@SIAT
 * 
 * Last Updated:2013-4-7 8:09:20
 * 
 * email: gh.chen@siat.ac.cn
 */

//import storm.realTraffic.bolt.DBWriterBolt;
import storm.realTraffic.bolt.DBWriter;
import storm.realTraffic.bolt.MapMatchingBolt;
import storm.realTraffic.bolt.SpeedCalculatorBolt;
import storm.realTraffic.bolt.SpeedCalculatorBolt2;
import storm.realTraffic.spout.FieldListenerSpout;
import storm.realTraffic.spout.SocketSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;



public class RealTimeTrafficTopology {
    
    public static void main(String[] args) throws AlreadyAliveException, 
                                                   InvalidTopologyException, 
                                                   InterruptedException 
                                                   {
    	// FieldListenerSpout fieldListenerSpout = new FieldListenerSpout();
    	SocketSpout socketSpout=new SocketSpout();
    	MapMatchingBolt districtMacthingBolt=new MapMatchingBolt(); 
    	//SpeedCalculatorBolt spdBolt =new SpeedCalculatorBolt();
    	SpeedCalculatorBolt2 spdBolt =new SpeedCalculatorBolt2();
    	DBWriter dbWriter=new DBWriter();
    	TopologyBuilder builder = new TopologyBuilder();

    	//builder.setSpout("spout", fieldListenerSpout,1);
    	builder.setSpout("spout", socketSpout,1);
    	builder.setBolt("RoadMatchingBolt", districtMacthingBolt,4).shuffleGrouping("spout");	        
    	builder.setBolt("SpeedCalculatorBolt",spdBolt,2).fieldsGrouping("RoadMatchingBolt",new Fields("roadID")); 
    	builder.setBolt("DBwriter",dbWriter,1).shuffleGrouping("SpeedCalculatorBolt"); 
    	
    	Config conf = new Config();
    	if(args!=null && args.length > 0) {
    		conf.setNumWorkers(60);            

    		LocalCluster  cluster= new LocalCluster();
    		cluster.submitTopology(args[0], conf, builder.createTopology());
    		//StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    	} 
        else {     

              conf.setDebug(true);
              conf.setMaxTaskParallelism(60);
              LocalCluster cluster = new LocalCluster();
              cluster.submitTopology(
              "Threshold_Test", conf, builder.createTopology());
    	      Thread.sleep(3000);
    	      cluster.shutdown(); 
        }

    }

}
