package com.perftek.event.simulator.model;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Event implements Serializable,Comparable {
	/* Business Transaction Name 
	 * Name of business transaction which needs to be monitored end to end 
	 */
	public String businessTransactionName;
	
	/* Transaction ID 
	 * Unique Transaction ID which gets carried through the business flow
	 */
	public String transactionID;
	
	/* Event name 
	 * Name of event, multiple per transaction. Need to clearly define which event is indication of start of transaction and which possible event(s) indicate end of transaction 
	 */
	public String eventName;
	
	/* Event Time 
	 * Time event took place (Not the time event was stored in Datababase or processed by Collector)
	 */
	public Date eventTime;
	
	/* Event Message ID 
	 * Message ID which is unique for event
	 */
	public String eventMessageID;
	
	/* Related Message ID 
	 */
	public String relatesToMessageID;
	
	/* machine Name 
	 * machine where event took place
	 */
	public String machinename;
	
	/* Component name 
	 * Component Where Evenet took place
	 */
	public String componentName;

	/* name of Component being called 
	 * Component Where call is headed
	 */
	public String callToComponent;
	
	
	/* type of event 
	 * START, FINISH, CALL, ASYNC
	 */
	public String eventType;
	public static final String START = "START";
	public static final String FINISH = "FINISH";
	public static final String ERROR = "ERROR";
	public static final String ASYNC = "ASYNC";
	public static final String CALL = "CALL";
	public static final String CALLFINISH = "CALLFINISH";
	
	/* 
	 * Time taken , populate for FINISH event types
	 */
	public long timeTaken;

	
	/* Attributes 
	 * various key values which might be usefull 
	 */
	public Map<String, String> attributes;
	
	/* Payload */
	public byte [] payload;
	
	public void addAttribute(String key, String value) {
		if ( attributes == null ) {
			attributes = new HashMap<String, String>();
		}
		attributes.put(key, value);
	}

	@Override
	public int compareTo(Object arg0) {
		return (int)(Math.random()*100);
	}
}
