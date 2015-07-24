package com.perftek.event.simulator.collector;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import jline.internal.Log;

import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.BufferUtils;
import org.apache.commons.collections.buffer.BoundedFifoBuffer;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import com.perftek.event.simulator.model.Event;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;

public class TitanWorker extends CollectorWorker{
	
	Logger LOG = Logger.getLogger(TitanWorker.class);
	
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
	private SimpleDateFormat sdfDisplay = new SimpleDateFormat("HH:mm:ss.SSS");
	static boolean initialized = false;
	static boolean closed = false;
	private Buffer events;

	
	public TitanWorker(Buffer events) {
		this.events = events;
	}
	

	TitanGraph  graph =null;
	GraphTraversalSource g = null;
	static final Date startdate = new Date();
	
	public void close() {
		try {
			closed = true;
			graph.close();
		} catch (Exception e) {
			LOG.error("error in close",e);
		}
	}
	
	public void connect(String node) {
		graph = TitanFactory.open("C:\\ws\\titan-0.9.0-M2-hadoop1\\conf\\titan-cassandra-es.properties");
		g = graph.traversal();
	}
	
	
	
	private Vertex getComponentVertex(String componentName, String tid) {
			Vertex compVertex = null;
			try {
				
				LOG.trace(componentName + ":" + tid + " SEARCHING");
				GraphTraversal gt2 = g.V().has(T.label, "component").has( "cid",componentName + ":" + tid);
				if ( gt2.hasNext() == false) { 
					LOG.trace(componentName + ":" + tid + " addVertex");
					compVertex = g.addV( T.label, "component", "cid",componentName + ":" + tid,"name", componentName,"tid",tid).next();
					
					LOG.trace(componentName + ":" + tid + " before commit");
					g.tx().commit();
					LOG.trace(componentName + ":" + tid + " commit done");
					LOG.trace(componentName + ":" + tid + " commit done 2");
					
				} else {
					compVertex = (Vertex) gt2.next();
				}
				
				LOG.trace(componentName + ":" + tid + " RETURNING " + compVertex);
				return compVertex;
				
			} catch (Throwable th) {
				LOG.trace("******************" + Thread.currentThread().getName() + "->" + th + "****************** " + componentName + ":" + tid + " :compVertex-> " + compVertex, th);
				g.tx().rollback();
			}
		return null;
	}
	
	
	public void insertEvent(Event event) {
		eventsProcessed++;
		Vertex compVertex = getComponentVertex(event.componentName,event.transactionID);//;graph.traversal().V(compMap.get(event.componentName).id()).next();//compMap.get(event.componentName);//graph.traversal().V().has(T.id,event.componentName + event.transactionID).next();
		if ( compVertex == null ) {
			events.add(event);
			graph.tx().commit();
			return;
		}
		Vertex calledComponentVertex = null;
		if (event.callToComponent != null ) {
			calledComponentVertex = getComponentVertex(event.callToComponent,event.transactionID);
			if ( calledComponentVertex == null ) {
				events.add(event);
				graph.tx().commit();
				return;
			}
		}
		
		if ( "TRANS_START_EVENT".equals(event.eventName) && Event.START.equals(event.eventType)) {
			Vertex busTran = graph.addVertex(T.label, "bustran", "name", "NTX", "tid",event.transactionID, "time", event.eventTime, "timeM", sdf.format(event.eventTime));
			busTran.addEdge("starter", compVertex,"time",event.eventTime,"tid",event.transactionID);
		}
		if ( Event.CALL.equals(event.eventType) || Event.ASYNC.equals(event.eventType) || Event.CALLFINISH.equals(event.eventType)) {
			if ( Event.CALLFINISH.equals(event.eventType) ) {
				calledComponentVertex.addEdge(event.eventType,compVertex ,"name",event.eventName, "time",event.eventTime,"tid",event.transactionID);
			} else {
				compVertex.addEdge(event.eventType, calledComponentVertex,"name",event.eventName, "time",event.eventTime,"tid",event.transactionID);
			}

		} else {
			Vertex eventVertex = graph.addVertex(T.label, "event", "name", event.eventName, "tid",event.transactionID,"host", event.machinename,"time",event.eventTime, "etype", event.eventType);
			compVertex.addEdge(event.eventType, eventVertex,"name",event.eventName, "time",event.eventTime,"tid",event.transactionID);
			
		}
		graph.tx().commit();
	}
	
}
