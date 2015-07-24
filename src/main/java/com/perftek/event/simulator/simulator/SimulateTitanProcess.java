package com.perftek.event.simulator.simulator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.BufferUtils;
import org.apache.commons.collections.buffer.BoundedBuffer;
import org.apache.commons.collections.buffer.BoundedFifoBuffer;
import org.apache.commons.collections.buffer.PriorityBuffer;
import org.apache.log4j.Logger;

import com.perftek.event.simulator.collector.Collector;
import com.perftek.event.simulator.collector.TitanWorker;
import com.perftek.event.simulator.producer.GeneratorGraph;

public class SimulateTitanProcess extends Simulate {
	static Logger LOG = Logger.getLogger(SimulateTitanProcess.class);
	static final Buffer events = BoundedBuffer.decorate(new PriorityBuffer(), 10000000, 10000);
	static final int numOfGenThreads = 100;
	static final List<Thread> generators = new ArrayList<Thread>(numOfGenThreads);
	static final int numOfCollThreads = 2;
	static final List<Thread> collectors = new ArrayList<Thread>(numOfCollThreads);
	static final int numbOfEventsToGenerate = 2;
	
	protected int getNumOfGenThreads() {return numOfGenThreads;}
	protected int getNumOfCollThreads() { return numOfCollThreads;}
	protected int getNumOfEventsToGenerate() {return numbOfEventsToGenerate;} 
	protected Buffer getEvents() { return events;}
	
	protected Collector getNewCollector(Buffer events) {
		return new Collector(new TitanWorker(events),events);
	}
	
	public static void main(String[] args) throws InterruptedException {
		LOG.debug("Starting Simulate Job");
		SimulateTitanProcess titan = new SimulateTitanProcess();
		titan.execute(args);
	}

}
