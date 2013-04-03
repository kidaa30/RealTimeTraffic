package storm.realTraffic;

/**
 * Copyright 2013 Xdata@SIAT
 * 
 * Last Updated:2013-1-7 8:09:20
 * 
 * email: gh.chen@siat.ac.cn
 */

//import storm.realTraffic.bolt.DBWriterBolt;
import storm.realTraffic.bolt.MapMatchingBolt;
import storm.realTraffic.bolt.SpeedCalculatorBolt;
import storm.realTraffic.spout.FieldListenerSpout;
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
    FieldListenerSpout fieldListenerSpout = new FieldListenerSpout();

	MapMatchingBolt districtMacthingBolt=new MapMatchingBolt(); 
	SpeedCalculatorBolt spdBolt =new SpeedCalculatorBolt();
//	DBWritterBolt dbWriterBolt = new DBWritterBolt();	


        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", fieldListenerSpout,1);	        
        builder.setBolt("matchingBolt", districtMacthingBolt,54).shuffleGrouping("spout");	        
       // builder.setBolt("countBolt",countBolt,6).shuffleGrouping("matchingBolt"); 
        builder.setBolt("countBolt",spdBolt,4).fieldsGrouping("matchingBolt",new Fields("roadID")); 
       //builder.setBolt("dbBolt",dbWriterBolt,2).shuffleGrouping("countBolt");
	    Config conf = new Config();
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(60);            

            //LocalCluster  cluster= new LocalCluster();
            //cluster.submitTopology(args[0], conf, builder.createTopology());
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
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
