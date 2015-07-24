package com.perftek.event.simulator.simulator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.BufferUtils;
import org.apache.commons.collections.buffer.BoundedBuffer;
import org.apache.commons.collections.buffer.BoundedFifoBuffer;
import org.apache.commons.collections.buffer.PriorityBuffer;

import com.perftek.event.simulator.collector.Collector;
import com.perftek.event.simulator.producer.GeneratorGraph;

public class Simulate {

//	static final Buffer events = BoundedBuffer.decorate(new PriorityBuffer(), 10000000, 10000);
//	static final int numOfGenThreads = 2;
	static final List<Thread> generators = new ArrayList<Thread>();
//	static final int numOfCollThreads = 1;
	static final List<Thread> collectors = new ArrayList<Thread>();
//	static final int numbOfEcentsToGenerate = 10;
	
	protected int getNumOfGenThreads() {return 0;}
	protected int getNumOfCollThreads() { return 0;}
	protected int getNumOfEventsToGenerate() {return 0;} 
	protected Buffer getEvents() { return null;}
	
	protected Collector getNewCollector(Buffer events) {
		return null;
	}
	
	public void execute(String[] args) throws InterruptedException {
	
		/*
		 * create buffer
		 * 
		 * create generator threads passing buffer
		 * 
		 * create collector threads passing buffer
		 * 
		 * 
		 * start generator threads
		 * 
		 * 
		 * start collector threads
		 * 
		 * 
		 */

//		events = BufferUtils.synchronizedBuffer(new BoundedFifoBuffer(1000));
//		events = BoundedBuffer.decorate(new PriorityBuffer(), 1000, 10000);
		long startTime = System.currentTimeMillis();
		long collectorsStartTime = -1;
		int totalErrors = 0;
		long totalEventsProcessed = 0;
		try {
			for (int i = 0; i < getNumOfGenThreads(); i++ ) {
				generators.add(new GeneratorGraph(getEvents(),getNumOfEventsToGenerate()));
			}
			
			for (int i = 0; i < getNumOfCollThreads(); i++ ) {
				Collector collector = getNewCollector(getEvents());//new Collector(events);
				collector.setDaemon(true);
				collectors.add(collector);
			}
			
			for (int i = 0; i < getNumOfGenThreads(); i++ ) {
				generators.get(i).start();
			}
			//wait for 5 seconds before starting collector threads
			Thread.currentThread().sleep(2000);
			
			collectorsStartTime = System.currentTimeMillis();
			for (int i = 0; i < getNumOfCollThreads(); i++ ) {
				collectors.get(i).start();;
			}
			

			while ( true) {
				for (Iterator iterator = collectors.iterator(); iterator.hasNext();) {
					Collector collectorThread = (Collector) iterator
							.next();
					
					
					if ( collectorThread.isDone()) {
						System.out.println("thread done");
						totalErrors = totalErrors + collectorThread.getErrorCount();
						totalEventsProcessed = totalEventsProcessed + collectorThread.getEventsProcessed();
						iterator.remove();
						
						if ( collectors.size() == 0) {
							System.out.println("Simulate time taken (ms): " + (System.currentTimeMillis() - startTime) + " to add " + totalEventsProcessed + " out of total " + (getNumOfGenThreads()*getNumOfEventsToGenerate()) + " business transaction with collector time to be " +  (System.currentTimeMillis() - collectorsStartTime) + " ms totalErrors= " + totalErrors);
							collectorThread.cleanup();
						}
					}
				}	
				if ( collectors.size() == 0) 
					break;
				
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
		} finally {
			System.out.println("Simulate time taken (ms): " + (System.currentTimeMillis() - startTime) + " to add " + (getNumOfGenThreads()*getNumOfEventsToGenerate()) + " business transaction with collector time to be " +  (System.currentTimeMillis() - collectorsStartTime) + " ms totalErrors= " + totalErrors);
		}
		
		
		
	}

}
