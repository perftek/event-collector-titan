package com.perftek.event.simulator.collector;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

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



public class TinkerWorker {
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
	private SimpleDateFormat sdfDisplay = new SimpleDateFormat("HH:mm:ss.SSS");
	static boolean initialized = false;
	static boolean closed = false;
	int errors = 0;
	static Graph graph =null;
	static final Date startdate = new Date();
	
	public int getErrorCount() {
		return errors;
	}
	
	public void close() {
		try {
			graph.io(IoCore.graphml()).writeGraph("/tmp/tinkerpop-modern.graphml");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		GraphTraversal gt3 = graph.traversal().V().has(T.label, "bustran").has("name", "NTX").has("time", P.between(startdate,new Date()));//, new Date()) );
		System.out.println("gt3");
		
		System.out.println(gt3.toList());
		
		GraphTraversal gt4 = graph.traversal().E().hasLabel("starter").has("time", P.between(startdate,new Date())).subgraph("subGraph");// composite index on label and time
		Object o = gt4.cap("subGraph").next();
		System.out.println(o.getClass());
		Graph subGraph = (Graph)o;
		GraphTraversalSource gts = subGraph.traversal();
		Graph subGraphT = gts.getGraph().get();
		try {
			subGraphT.io(IoCore.graphml()).writeGraph("/tmp/tinkerpop-subGraph.graphml");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		GraphTraversal gt5 = graph.traversal().V().has(T.label, "bustran").has("name", "NTX").has("time", P.between(startdate,new Date())); // composite index on vertex: label, name, time 
		List<Vertex> bustrans = gt5.toList();
		for (Iterator<Vertex> iterator = bustrans.iterator(); iterator.hasNext();) {
			Vertex object = iterator.next();
			Graph subGrapthperT =(Graph) graph.traversal().V(object).repeat(__.outE().subgraph("subGraph").inV()).times(6).cap("subGraph").next();
			try {
				subGrapthperT.io(IoCore.graphson()).writeGraph("/tmp/tinkerpop-subGraph_" + object.property("tid").value() + ".graphml");
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			List<Vertex> nodes = subGrapthperT.traversal().V().toList();
			for (Iterator<Vertex> iterator2 = nodes.iterator(); iterator2.hasNext();) {
				Vertex vertex = iterator2.next();
				String id = (String)vertex.id();
				if (vertex.label().equals("event")) {
					
				} else {
					List<Edge> outEdgesSTART = subGrapthperT.traversal().V(vertex).outE().hasLabel("START").toList();
					List<Edge> outEdgesFINISH = subGrapthperT.traversal().V(vertex).outE().hasLabel("FINISH").toList();
					System.out.println("\"" + id.substring(0, id.lastIndexOf(':'))  + "\": {\"START\": \"" 
					+ (outEdgesSTART.size() > 0 ? sdfDisplay.format((Date)outEdgesSTART.get(0).property("time").value()) : "")  
					+ "\",\"FINISH\": \"" + (outEdgesFINISH.size() > 0 ? sdfDisplay.format((Date)outEdgesFINISH.get(0).property("time").value()) : "")
					+ "\",\"time\": \"" + (outEdgesSTART.size() > 0 && outEdgesFINISH.size() > 0 ? ( ((Date)outEdgesFINISH.get(0).property("time").value()).getTime() - ((Date)outEdgesSTART.get(0).property("time").value()).getTime()  ) + " ms" : "")
					+ "\"},");
					
				}
			}
			
			List<Edge> edges = subGrapthperT.traversal().E().toList();
			for (Iterator<Edge> iterator2 = edges.iterator(); iterator2.hasNext();) {
				Edge edge = iterator2.next();
				String src = ((String)edge.outVertex().id()).substring(0, ((String)edge.outVertex().id()).lastIndexOf(':'));
				String trg = ((String)edge.inVertex().id()).substring(0, ((String)edge.inVertex().id()).lastIndexOf(':'));
				
				if ( edge.label().equals("CALLFINISH") ) {
					List<Edge> callEdges = subGrapthperT.traversal().V(edge.inVertex().id()).outE("CALL").toList();
					if ( callEdges.size() > 0) {
						Date callDate = (Date)callEdges.get(0).property("time").value();
						Date finishDate = (Date)edge.property("time").value();
						System.out.println("g.setEdge(\"" + src + "\",\"" +trg + "\", { labelType: \"html\", label: \"<div><span style=\\\"color: #fff;\\\">" + edge.label() + "<BR>" + (finishDate.getTime() - callDate.getTime()) + "(ms)</span></div>\" , rx: 5,ry: 5, padding: 0});");
					}
				} else {
					if ( edge.label().equals("START") || edge.label().equals("FINISH")) {
						
					} else {
						System.out.println("g.setEdge(\"" + src + "\",\"" +trg + "\",     { label: \"" + edge.label() + "\" });");
					}
				}
			}
		}
		
	}
	
	public void connect(String node) {
		TinkerWorker.init();
	}
	
	private static synchronized void init() {
		if (initialized)
			return;
		
		synchronized (TinkerWorker.class) {
			graph = TinkerGraph.open();
			initialized = true;	
		}
		
	}
	
	public void insertEvent(Event event) {
		System.out.println("TinkerWorker:" + event.componentName + " " + event.eventName + " " + event.eventType + " " + event.transactionID);

		Vertex compVertex = null;//graph.traversal().V().has(T.id,event.componentName + event.transactionID).next();
		
		GraphTraversal gt2 = graph.traversal().V(event.componentName + ":" + event.transactionID); // search by vertex ID, automatic indexed I guess
		List<Vertex> compVertexList = gt2.toList();
		System.out.println(event.componentName + ":" + event.transactionID + " " + compVertexList.size());
		if ( compVertexList != null && compVertexList.size() > 0) {
			compVertex = compVertexList.get(0); 
		} else {
			compVertex = graph.addVertex(T.id, event.componentName + ":" + event.transactionID, T.label, "component", "name", event.componentName, "tid",event.transactionID);
		}
		
		
		if ( "TRANS_START_EVENT".equals(event.eventName) && Event.START.equals(event.eventType)) {
			Vertex busTran = graph.addVertex(T.id, "bustran:NRX" + ":" + event.transactionID,T.label, "bustran", "name", "NTX", "tid",event.transactionID, "time", event.eventTime);
			busTran.addEdge("starter", compVertex,"time",event.eventTime);
		}
		
		if ( Event.CALL.equals(event.eventType) || Event.ASYNC.equals(event.eventType) || Event.CALLFINISH.equals(event.eventType)) {
			GraphTraversal gt = graph.traversal().V().has(T.id,event.callToComponent + ":" + event.transactionID); // ID should be indexed

			Vertex calledComponentVertex = null;
			List<Vertex> nodes = gt.toList();
			int size = nodes.size();
			System.out.println(size);			
			if (size > 0 )
				calledComponentVertex = nodes.get(0);
			if ( calledComponentVertex == null ){
				calledComponentVertex = graph.addVertex(T.id, event.callToComponent + ":" + event.transactionID,T.label, "component", "name", event.callToComponent, "tid",event.transactionID);
				System.out.println("added vertex " + calledComponentVertex + " -> " + calledComponentVertex.keys());
			}
			
			if ( Event.CALLFINISH.equals(event.eventType) ) {
				calledComponentVertex.addEdge(event.eventType,compVertex ,"name",event.eventName, "time",event.eventTime);
			} else {
				compVertex.addEdge(event.eventType, calledComponentVertex,"name",event.eventName, "time",event.eventTime);
			}
		} else {
			Vertex eventVertex = graph.addVertex(T.id, event.eventName + ":" + event.eventType + ":" + event.transactionID, T.label, "event", "name", event.eventName, "tid",event.transactionID,"host", event.machinename,"time",event.eventTime, "etype", event.eventType);
			compVertex.addEdge(event.eventType, eventVertex,"name",event.eventName, "time",event.eventTime);
		}
		
	}
	
}
