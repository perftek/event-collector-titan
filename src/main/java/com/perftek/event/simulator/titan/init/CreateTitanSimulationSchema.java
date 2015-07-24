package com.perftek.event.simulator.titan.init;

import java.util.Date;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.VertexLabel;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;

public class CreateTitanSimulationSchema {
	public static final String INDEX_NAME = "eventSearch";

    public static void load(final TitanGraph graph) {
        load(graph, INDEX_NAME, true);
    }

    public static void load(final TitanGraph graph, String mixedIndexName, boolean uniqueNameCompositeIndex) {
        //Create Schema
    	
		// vertex properties 
		// name
		// tid
		// label (indexed or not?)
		// time
		// host
		// etype
		// composite index on vertex: label, name, time
    	// composite index on vertex:  name, tid
    	
        TitanManagement mgmt = graph.openManagement();
        try {
        final PropertyKey vertexID = mgmt.makePropertyKey("cid").dataType(String.class).make();
        final PropertyKey eventName = mgmt.makePropertyKey("name").dataType(String.class).make();
        final PropertyKey eventTid = mgmt.makePropertyKey("tid").dataType(String.class).make();
        final PropertyKey time = mgmt.makePropertyKey("time").dataType(Date.class).make();
        final PropertyKey timeM = mgmt.makePropertyKey("timeM").dataType(String.class).make();
        final PropertyKey host = mgmt.makePropertyKey("host").dataType(String.class).make();
        final PropertyKey etype = mgmt.makePropertyKey("etype").dataType(String.class).make();
        
        VertexLabel bustran = mgmt.makeVertexLabel("bustran").make();
        VertexLabel component = mgmt.makeVertexLabel("component").make();
        mgmt.makeVertexLabel("event").make();

        
        mgmt.buildIndex("byNameAndTimeM",Vertex.class).addKey(eventName).addKey(timeM).indexOnly(bustran).buildCompositeIndex();

        TitanManagement.IndexBuilder cidIndexBuilder = mgmt.buildIndex("cidIndex", Vertex.class).addKey(vertexID).indexOnly(component).unique();
        TitanGraphIndex cidIndex = cidIndexBuilder.buildCompositeIndex();
        mgmt.setConsistency(cidIndex, ConsistencyModifier.LOCK);
        
        TitanManagement.IndexBuilder compNameIndexBuilder = mgmt.buildIndex("compNameIndex", Vertex.class).addKey(eventName).addKey(eventTid).indexOnly(component);
        TitanGraphIndex eventNamei = compNameIndexBuilder.buildCompositeIndex();
//        mgmt.setConsistency(eventNamei, ConsistencyModifier.LOCK);
        
        /*
        
        TitanManagement.IndexBuilder nameIndexBuilder = mgmt.buildIndex("labelNameTime", Vertex.class).addKey(tranName).addKey(tid);
        if (uniqueNameCompositeIndex)
            nameIndexBuilder.unique();
        TitanGraphIndex namei = nameIndexBuilder.buildCompositeIndex();
        mgmt.setConsistency(namei, ConsistencyModifier.LOCK);
        
        final PropertyKey tranName = mgmt.makePropertyKey("tranName").dataType(String.class).make();
        final PropertyKey tid = mgmt.makePropertyKey("tid").dataType(String.class).make();
        TitanManagement.IndexBuilder nameIndexBuilder = mgmt.buildIndex("tranName", Vertex.class).addKey(tranName).addKey(tid);
        if (uniqueNameCompositeIndex)
            nameIndexBuilder.unique();
        TitanGraphIndex namei = nameIndexBuilder.buildCompositeIndex();
        mgmt.setConsistency(namei, ConsistencyModifier.LOCK);
        
        
        
        final PropertyKey age = mgmt.makePropertyKey("age").dataType(Integer.class).make();
        if (null != mixedIndexName)
            mgmt.buildIndex("vertices", Vertex.class).addKey(age).buildMixedIndex(mixedIndexName);

        
        final PropertyKey reason = mgmt.makePropertyKey("reason").dataType(String.class).make();
        final PropertyKey place = mgmt.makePropertyKey("place").dataType(Geoshape.class).make();
        if (null != mixedIndexName)
            mgmt.buildIndex("edges", Edge.class).addKey(reason).addKey(place).buildMixedIndex(mixedIndexName);

         */
        
     // edge properties 
     		// time
     		// name
     		// composite index on edge: label and time
        
//        final PropertyKey edgeTime = mgmt.makePropertyKey("time").dataType(Date.class).make();
//        final PropertyKey edgeName = mgmt.makePropertyKey("name").dataType(String.class).make();

        //.multiplicity(Multiplicity.ONE2MANY)
        EdgeLabel ASYNClabel = mgmt.makeEdgeLabel("ASYNC").signature(eventTid).signature(time).make();
        EdgeLabel CALLlabel = mgmt.makeEdgeLabel("CALL").signature(eventTid).signature(time).make();
        EdgeLabel starter = mgmt.makeEdgeLabel("starter").signature(timeM).make();
        
    
        EdgeLabel CALLFINISHlabel = mgmt.makeEdgeLabel("CALLFINISH").signature(eventTid).signature(time).make();
        EdgeLabel FINISHlabel = mgmt.makeEdgeLabel("FINISH").signature(eventTid).signature(time).make();
        EdgeLabel STARTlabel = mgmt.makeEdgeLabel("START").signature(eventTid).signature(time).make();
        
        mgmt.buildEdgeIndex(starter, "starterByTime", Direction.BOTH, Order.decr, timeM);
        mgmt.buildEdgeIndex(starter, "starterByTID", Direction.BOTH, Order.decr, eventTid);
        mgmt.buildEdgeIndex(ASYNClabel, "ASYNClabelByTid", Direction.BOTH, Order.decr, eventTid);
        mgmt.buildEdgeIndex(CALLlabel, "CALLlabelByTid", Direction.BOTH, Order.decr, eventTid);
        mgmt.buildEdgeIndex(CALLFINISHlabel, "CALLFINISHlabelByTid", Direction.BOTH, Order.decr, eventTid);
        mgmt.buildEdgeIndex(FINISHlabel, "FINISHlabelByTid", Direction.BOTH, Order.decr, eventTid);
        mgmt.buildEdgeIndex(STARTlabel, "STARTlabelByTid", Direction.BOTH, Order.decr, eventTid);
        mgmt.commit();
        } catch (Throwable th) {
        	th.printStackTrace();
        	mgmt.rollback();
        }
        

    }
	
	public static void main(String[] args) {
		TitanGraph g = TitanFactory.open("C:\\ws\\titan-0.9.0-M2-hadoop1\\conf\\titan-cassandra-es.properties");
		try {
			load(g);
		} finally {
			g.close();
		}
	}

}
