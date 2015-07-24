package com.perftek.event.simulator.collector;

import org.apache.commons.collections.Buffer;
import org.apache.log4j.Logger;

import com.perftek.event.simulator.model.Event;

public class Collector extends Thread{
	Logger LOG = Logger.getLogger(Collector.class);
	Buffer events;
	long eventsProcessedCount = 0;
	CollectorWorker worker = null;
	boolean done = false;
	int errorCount = 0;
	long eventsProcessed = 0;
	
	public void cleanup() {
		worker.close();
	}
	
	public int getErrorCount() {
		return errorCount;
	}
	
	public long getEventsProcessed() {
		return eventsProcessed;
	}
	
	public Collector(CollectorWorker worker ,Buffer events) {
		this.worker = worker;
		this.events = events;
		worker.connect("127.0.0.1");
	}
	
	public void processEvent(Event event) {
		worker.insertEvent(event);
		
	}
	
	@Override
	public void run() {
		long startTime = System.currentTimeMillis();
		try {
			for (;;) {
				Event event = (Event)events.remove();
				processEvent(event);
				eventsProcessedCount++;
				if ( (eventsProcessedCount % 1000) == 0) {
					LOG.trace("" + this.getName() + " has processed " + eventsProcessedCount + " events in " + (System.currentTimeMillis() - startTime) + " ms");
				}
			}
		} finally {
			LOG.trace("" + this.getName() + " has processed " + eventsProcessedCount + " events in " + (System.currentTimeMillis() - startTime) + " ms");
			try {
				errorCount = worker.getErrorCount();
				eventsProcessed = worker.getEventsProcessed();
				worker.close();
			} catch (Throwable th) {
				LOG.error("run() exception",th);
			}
			done = true;
		}
		
	}

	public boolean isDone() {
		return done;
	}

}
