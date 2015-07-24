package com.perftek.event.simulator.collector;


import java.util.Date;

import org.apache.log4j.Logger;

import com.perftek.event.simulator.model.Event;



public class CollectorWorker {
	Logger LOG = Logger.getLogger(CollectorWorker.class);
	static boolean initialized = false;
	static boolean closed = false;
	protected int errors = 0;
	protected 	long eventsProcessed = 0;

	static final Date startdate = new Date();

	
	public int getErrorCount() {
		return errors;
	}
	
	public void close() {
	}
	
	public void connect(String node) {
	}
	
	
	public void insertEvent(Event event) {
	}
	public long getEventsProcessed() {
		return eventsProcessed;
	}
	
}
