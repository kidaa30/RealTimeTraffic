package storm.realTraffic.bolt;

import java.io.Serializable;
import storm.realTraffic.spout.TupleInfo;

public class thresholdInfo extends TupleInfo implements Serializable{
    
	/**
	 * 
	 */
	private static final long serialVersionUID = 3337988877646687347L;


	
	private String action;
    private String rule;
    private static Integer thresholdValue;
    private static int thresholdColNumber;
    private static Integer timeWindow;
    private int frequencyOfOccurence;
    
    public thresholdInfo(){
    	
    }
    
	public static int getThresholdColNumber() {
		// TODO Auto-generated method stub
		return thresholdColNumber;
	}
	public static Integer getThresholdValue() {
		// TODO Auto-generated method stub
		return thresholdValue;
	}
	public static int getFrequencyOfOccurence() {
		// TODO Auto-generated method stub
		return 0;
	}
	public static String getAction() {
		// TODO Auto-generated method stub
		return null;
	}
	public static Integer getTimeWindow() {
		// TODO Auto-generated method stub
		return timeWindow;
	}  

}
