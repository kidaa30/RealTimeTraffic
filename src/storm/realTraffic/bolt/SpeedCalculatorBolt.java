package storm.realTraffic.bolt;

import backtype.storm.Config;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import storm.realTraffic.spout.TupleInfo;

public class SpeedCalculatorBolt implements IRichBolt{ //Serializable {
	Tuple tuple;

	private OutputCollector collector;


   // @Override
    public void execute() {
     if(tuple!=null)
    {
        List<Object> inputTupleList = (List<Object>) tuple.getValues();
        int thresholdColNum = thresholdInfo.getThresholdColNumber();
        Object thresholdValue = thresholdInfo.getThresholdValue();
        String thresholdDataType =tupleInfo.getFieldList().get(thresholdColNum-1).getColumnType();
        Integer timeWindow = thresholdInfo.getTimeWindow();
        int frequency = thresholdInfo.getFrequencyOfOccurence();

        long startTime;

		if(thresholdDataType.equalsIgnoreCase("string"))
        {
            String valueToCheck = inputTupleList.get(thresholdColNum-1).toString();
            String frequencyChkOp = thresholdInfo.getAction();
            if(timeWindow!=null)
            {
                long curTime = System.currentTimeMillis();
                long diffInMinutes = (curTime-startTime)/(1000);
                if(diffInMinutes>=timeWindow)
                {
                    if(frequencyChkOp.equals("=="))
                    {
                         if(valueToCheck.equalsIgnoreCase(thresholdValue.toString()))
                         {
                             count.incrementAndGet();
                             if(count.get() > frequency)
                                 splitAndEmit(inputTupleList,collector);
                         }
                    }
                    else if(frequencyChkOp.equals("!="))
                    {
                        if(!valueToCheck.equalsIgnoreCase(thresholdValue.toString()))
                        {
                             count.incrementAndGet();
                             if(count.get() > frequency)
                                 splitAndEmit(inputTupleList,collector);
                         }
                     }
                     else
                         System.out.println("Operator not supported");
                 }
             }
             else
             {
                 if(frequencyChkOp.equals("=="))
                 {
                     if(valueToCheck.equalsIgnoreCase(thresholdValue.toString()))
                     {
                         count.incrementAndGet();
                         if(count.get() > frequency)
                             splitAndEmit(inputTupleList,collector);    
                     }
                 }
                 else if(frequencyChkOp.equals("!="))
                 {
                      if(!valueToCheck.equalsIgnoreCase(thresholdValue.toString()))
                      {
                          count.incrementAndGet();
                          if(count.get() > frequency)
                              splitAndEmit(inputTupleList,collector);    
                      }
                  }
              }
           }
           else if(thresholdDataType.equalsIgnoreCase("int") || 
                   thresholdDataType.equalsIgnoreCase("double") || 
                   thresholdDataType.equalsIgnoreCase("float") || 
                   thresholdDataType.equalsIgnoreCase("long") || 
                   thresholdDataType.equalsIgnoreCase("short"))
           {
               String frequencyChkOp = thresholdInfo.getAction();
               if(timeWindow!=null)
               {
                    long valueToCheck = 
                        Long.parseLong(inputTupleList.get(thresholdColNum-1).toString());
                    long curTime = System.currentTimeMillis();
                    long diffInMinutes = (curTime-startTime)/(1000);
                    System.out.println("Difference in minutes="+diffInMinutes);
                    if(diffInMinutes>=timeWindow)
                    {
                         if(frequencyChkOp.equals("<"))
                         {
                             if(valueToCheck < Double.parseDouble(thresholdValue.toString()))
                             {
                                  count.incrementAndGet();
                                  if(count.get() > frequency)
                                      splitAndEmit(inputTupleList,collector);
                             }
                         }
                         else if(frequencyChkOp.equals(">"))
                         {
                              if(valueToCheck > Double.parseDouble(thresholdValue.toString())) 
                              {
                                  count.incrementAndGet();
                                  if(count.get() > frequency)
                                      splitAndEmit(inputTupleList,collector);
                              }
                          }
                          else if(frequencyChkOp.equals("=="))
                          {
                             if(valueToCheck == Double.parseDouble(thresholdValue.toString()))
                             {
                                 count.incrementAndGet();
                                 if(count.get() > frequency)
                                     splitAndEmit(inputTupleList,collector);
                              }
                          }
                          else if(frequencyChkOp.equals("!="))
                          {
   				//. . . 
                          }
                      }
                  
             }
     else
          splitAndEmit(null,collector);
     }
     else
     {
          System.err.println("Emitting null in bolt");
          splitAndEmit(null,collector);
     }

    }     
   }
    
    
//    private void splitAndEmit(List<Object> inputTupleList,
//			OutputCollector collector2) {
//		// TODO Auto-generated method stub
//    	
//    	
//		
//	}


	@Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_outFields);
    }


	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		//this.counters = new HashMap<String, Integer>();
		this.collector = collector;
		//this.name = context.getThisComponentId();
		//this.id = context.getThisTaskId();
		
	}


	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}


	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}   
	
}
