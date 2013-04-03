package storm.realTraffic.spout;

//import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import storm.realTraffic.gis.GPSRcrd;
import storm.realTraffic.spout.TupleInfo;


public class FieldListenerSpout implements IRichSpout {
    private static final long serialVersionUID = 1L;
	private SpoutOutputCollector _collector;
    private BufferedReader fileReader;
    //private TopologyContext context;
    //private String file="/home/ghchen/2013-01-05.1/2013-01-05--11_05_48.txt";
    private TupleInfo tupleInfo=new TupleInfo();
    
    //Fields fields;
    
    static Socket sock=null;
    
    public void close() {
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) 
		{
		    _collector = collector;		    

	    
		 String file=new String();
			 if(file.equals(""))
			 {
				file="/home/hadoop/ghchen/GPS_2011_09_27.txt";
				 //file="D:\\siat-code\\GPS_2011_09_27-5000line.txt";
			 }
			 
		try 
			{	

			  this.fileReader = new BufferedReader(new FileReader(new File(file))); 
			} 
		catch (FileNotFoundException e) 
			{
				throw new RuntimeException ("error reading file ["+file+"]");
			}

		}


    @SuppressWarnings("unused")
	public void nextTuple() {    	

    	//Utils.sleep(1000);
    	// RandomAccessFile access = null; 
    	String line = null;  
		  BufferedReader access= new BufferedReader(fileReader);
           try 
           {  		   
               while ((line = access.readLine()) != null)
               { 
                   if (line !=null)
                   {
                  	   String[] GPSRecord =line.split(tupleInfo.getDelimiter());
						     //       line.split("\\"+tupleInfo.getDelimiter());							

                        if (tupleInfo.getFieldList().size() == GPSRecord.length)
                           {
                        	_collector.emit(new Values(GPSRecord)); 
                            //tupleInfo = new TupleInfo(GPSRecord);              
                           }                          
                   }          
               } 
           } catch (IOException ex) {System.out.println(ex); } 
    

    	/*int count=0;
    	int ch=0;
    	int err=0;
    	try {
    		if(sock==null){
    			sock=new Socket("172.20.14.204",15025);}
    		while(true){
				byte[] b3= new byte[3];
				if(sock!=null ){
					try{
						sock.getInputStream().read(b3,0,3);
						ch=b3[0];
					}catch ( Exception e){
						System.out.println("connection reset, reconnecting ...");
						sock.close();
						Thread.sleep(100);
						sock=new Socket("172.20.14.204",15025);						
					}

				}else{
					sock=new Socket("172.20.14.204",15025);	
					break ;
				}
    			int len=SocketJava.bytesToShort(b3, 1);
    			if(len<0) break;
    			byte[] bytelen= new byte[len];
    			sock.getInputStream().read(bytelen);
    			if(bytelen==null){
    				System.out.println("read the second part from byte from socket failed ! ");
    				break;
    			}
    			sock.getInputStream().markSupported();
    			sock.getInputStream().mark(3);

    			String gpsString=SocketJava.DissectOneMessage(ch,bytelen);
    			//System.out.println("GPSstring="+gpsString);
    			String[] GPSRecord=null;
    			if(gpsString!=null){
    				GPSRecord =gpsString.split(TupleInfo.getDelimiter());
    				//					for(int i=0;i<GPSRecord.length-1;i++)
    				//					writeToFile("/home/ghchen/gpsRecord",GPSRecord[i]+",");
    				//					writeToFile("/home/ghchen/gpsRecord","\n");					

    				_collector.emit(new Values(GPSRecord[0],GPSRecord[3],GPSRecord[7],GPSRecord[5],
    						GPSRecord[6] , GPSRecord[2],GPSRecord[1])); 
    				//}

    			}else{
    				break;
    			}

    		}	
    	} catch (IOException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	} catch (Exception e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}	*/



    }        

    public void ack(Object id) {
    	System.out.println("OK:"+id);
    }
    

    public void fail(Object id) {
    	System.out.println("Fail:"+id);
    }    

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
 
    	TupleInfo tuple = new TupleInfo();
    	Fields fieldsArr;
    	try {
    		fieldsArr= tuple.getFieldList(); 
    		declarer.declare(fieldsArr);

		} catch (Exception e) {
			// TODO: handle exception
			throw new RuntimeException("error:fail to new Tuple object in declareOutputFields, tuple is null",e);  
		}    	  		

    }

	public void activate() {
		// TODO Auto-generated method stub

	}

	public void deactivate() {
		// TODO Auto-generated method stub

	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	static int count=0;
	public static void writeToFile(String fileName, Object obj){
		try {
			//count=count+1;
			FileWriter fwriter;
			fwriter= new FileWriter(fileName,true);
		     BufferedWriter writer= new BufferedWriter(fwriter);

		      	writer.write(obj.toString());

		      //writer.write("\n\n");
		      writer.close(); 

		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}		
	}



    
}