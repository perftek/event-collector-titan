package com.perftek.event.simulator.producer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.collections.Buffer;




import com.perftek.event.simulator.model.Event;

public class GeneratorGraph extends Thread{
	Buffer events;
//	List<Event> events = new ArrayList<Event>(1000000);
	int numbOfEcentsToGenerate;
	int minutesToAdd = 0;
		

	
	public GeneratorGraph(Buffer events,int numbOfEcentsToGenerate) {
		this.events = events;
		this.numbOfEcentsToGenerate = numbOfEcentsToGenerate;
	}


	
	
	public void createEvents() {
		Map<String, Object> transactionData = new HashMap<String, Object>();
		createDataForEvent(transactionData );
		String transactionID = (String )transactionData.get("transactionID");
		
		LocalDateTime ldt = LocalDateTime.now().plusMinutes(minutesToAdd);//minusSeconds((long)(Math.random()*6000));
	
		events.add(creatEvent(transactionID,ldt,"NTX","TRANS_START_EVENT","MACHINE001","COMPONENT001",transactionData,Event.START,null,0));
		long msToAdd = 0;
		msToAdd = msToAdd + (long)((100)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT001","MACHINE001","COMPONENT001",transactionData,Event.ASYNC,"COMPONENT002",100));
		msToAdd = msToAdd + (long)((100)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","TRANS_START_EVENT","MACHINE001","COMPONENT001",transactionData,Event.FINISH,null,100));
		

		msToAdd = msToAdd + (long)((1000)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT002","MACHINE002","COMPONENT002",transactionData,Event.START,null,0));
		msToAdd = msToAdd + (long)((1000)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT003","MACHINE002","COMPONENT002",transactionData,Event.CALL,"COMPONENT003",0));
		msToAdd = msToAdd + (long)((500)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT003","MACHINE003","COMPONENT003",transactionData,Event.START,null,0));
		msToAdd = msToAdd + (long)((1000)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT003","MACHINE003","COMPONENT003",transactionData,Event.FINISH,null,100));
		msToAdd = msToAdd + (long)((1000)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT003","MACHINE002","COMPONENT002",transactionData,Event.CALLFINISH,"COMPONENT003",0));
		msToAdd = msToAdd + (long)((100)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT004","MACHINE002","COMPONENT002",transactionData,Event.ASYNC,"COMPONENT004",120));
		msToAdd = msToAdd + (long)((1000)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT002","MACHINE002","COMPONENT002",transactionData,Event.FINISH,null,121));
		
		
		msToAdd = msToAdd + (long)((1000)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT005","MACHINE005","COMPONENT004",transactionData,Event.START,null,0));
		msToAdd = msToAdd + (long)((1000)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT006","MACHINE005","COMPONENT004",transactionData,Event.CALL,"COMPONENT005",10));
		msToAdd = msToAdd + (long)((1000)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT007","MACHINE001","COMPONENT005",transactionData,Event.START,null,0));
		msToAdd = msToAdd + (long)((1000)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT007","MACHINE001","COMPONENT005",transactionData,Event.CALL,"COMPONENT006",1));
		msToAdd = msToAdd + (long)((1000)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT008","MACHINE004","COMPONENT006",transactionData,Event.START,null,10));
		msToAdd = msToAdd + (long)((1000)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT008","MACHINE004","COMPONENT006",transactionData,Event.FINISH,null,120));
		msToAdd = msToAdd + (long)((1000)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT007","MACHINE001","COMPONENT005",transactionData,Event.CALLFINISH,"COMPONENT006",1));
		msToAdd = msToAdd + (long)((1000)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT007","MACHINE001","COMPONENT005",transactionData,Event.FINISH,null,122));
		msToAdd = msToAdd + (long)((1000)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT006","MACHINE005","COMPONENT004",transactionData,Event.CALLFINISH,"COMPONENT005",10));
//		msToAdd = msToAdd + (long)((10)* Math.random());
//		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","MB_NEW_RX_RT_TO_CH_DLV","MACHINE005","COMPONENT004",transactionData));
		msToAdd = msToAdd + (long)((1000)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT009","MACHINE005","COMPONENT004",transactionData,Event.ASYNC,"COMPONENT007",220));
		msToAdd = msToAdd + (long)((1000)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT005","MACHINE005","COMPONENT004",transactionData,Event.FINISH,null,230));
		
		
		msToAdd = msToAdd + (long)((1000)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT010","MACHINE006","COMPONENT007",transactionData,Event.START,null,0));
		msToAdd = msToAdd + (long)((1000)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT011","MACHINE006","COMPONENT007",transactionData,Event.CALL,"COMPONENT009",0));
		msToAdd = msToAdd + (long)((1000)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT011","MACHINE006","COMPONENT007",transactionData,Event.CALLFINISH,"COMPONENT009",120));
		msToAdd = msToAdd + (long)((1000)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT012","MACHINE006","COMPONENT007",transactionData,Event.ASYNC,"COMPONENT008",190));
		msToAdd = msToAdd + (long)((1000)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT010","MACHINE006","COMPONENT007",transactionData,Event.FINISH,null,200));

		
		msToAdd = msToAdd + (long)((1000)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT013","MACHINE001","COMPONENT008",transactionData,Event.START,null,0));
		msToAdd = msToAdd + (long)((1000)* Math.random());
		events.add(creatEvent(transactionID,ldt.plus(msToAdd,ChronoUnit.MILLIS),"NTX","EVENT013","MACHINE001","COMPONENT008",transactionData,Event.FINISH,null,120));
		/**/
	}
	
	
	private void createDataForEvent(Map<String, Object> dataForPayload) {
		String transactionID = UUID.randomUUID().toString();
		dataForPayload.put("transactionID", transactionID);
		dataForPayload.put("messageID", transactionID);
		dataForPayload.put("msgId", transactionID);
	}

	@Override
	public void run() {
		System.out.println("Generator thread started " + this.getName());
		long startTime = System.currentTimeMillis();
		for ( int i = 0; i < numbOfEcentsToGenerate; i++) {
			createEvents();
			if ( (i % 40) == 0) {
				System.out.println("" + this.getName() + " has generated " + i + " events in " + (System.currentTimeMillis() - startTime) + " ms");
				minutesToAdd=minutesToAdd+1;
//				try {
//					if ( i != 0)
//						Thread.sleep(60*1000); // sleep for 1 minute
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
			}
		}
		System.out.println("Generator thread done " + this.getName());
	}
	
	private Event creatEvent(String transactionID, LocalDateTime eventTime, String businessTransactionName, String eventName, String machinename, String componentName,Map<String, Object> dataForPayload) {
		return creatEvent(transactionID, eventTime, businessTransactionName, eventName, machinename, componentName,dataForPayload, null, null,0);
	}
	
	private Event creatEvent(String transactionID, LocalDateTime eventTime, String businessTransactionName, String eventName, String machinename, String componentName,Map<String, Object> dataForPayload, String eventType, String callToComponent, long timeTaken) {
		Event event = new Event();
		event.businessTransactionName = businessTransactionName;
		event.transactionID = transactionID;
		event.eventTime = Date.from(eventTime.atZone(ZoneId.systemDefault()).toInstant());
		event.eventName = eventName;
		event.machinename = machinename;
		event.componentName = componentName;
		event.callToComponent = callToComponent;
		event.eventType = eventType;
		event.timeTaken = timeTaken;
		return event;
		
	}
	
}
