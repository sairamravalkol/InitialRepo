package tvg.hpl.hp.com.ws;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.codehaus.jettison.json.JSONObject;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONWriter;

public class Test {

	Map<String, List<List<String>>> subGraph = new HashMap<>();
	private Connection connection;
	private PreparedStatement pstmt;
	private ResultSet resultset;

	public static void main(String[] args) throws FileNotFoundException, IOException {
		Map<String, List<List<String>>> testGraph= 	new Test().getSubgraphWithTaskId(48);
		Graph graph = new TinkerGraph();
		
	//	System.out.println(testGraph);
		Set<Entry<String,List<List<String>>>> entrySet = testGraph.entrySet();
	//	System.out.println(entrySet);
		
		for(Map.Entry<String, List<List<String>>> subgraph : testGraph.entrySet()) {
			
			String source = subgraph.getKey();
			Iterable<Vertex> iterable1 = graph.getVertices();
			Set<String> vertexSet1 = Test.getVertexList(iterable1);
			Vertex outV=null;
			if(!vertexSet1.contains(source)){
			outV = graph.addVertex(source);
			outV.setProperty("black_list", false);
			outV.setProperty("external_flag", false);
			outV.setProperty("machine_type", "personal");
			outV.setProperty("page_rank", 0.124535353);
			}
			else{
			 outV = graph.getVertex(source.toString());
			}
			
			List<List<String>> list = subgraph.getValue();
			List<String> destinations = list.get(0);
			List<String> epochs = list.get(1);
			
			
				
			System.out.println("destination size:"+destinations.size());
			for (int i=0; i< destinations.size(); i++) {
			//	System.out.println("source:"+source +" " + "des: "+destinations.get(i)+ " "+ "time:"+epochs.get(i));
				
				Iterable<Vertex> iterable = graph.getVertices();
				Set<String> vertexSet = Test.getVertexList(iterable);
				if(!vertexSet.contains(destinations.get(i))){
				
				Vertex inV = graph.addVertex(destinations.get(i));
				inV.setProperty("black_list", false);
				inV.setProperty("external_flag", false);
				inV.setProperty("machine_type", "personal");
				inV.setProperty("page_rank", 0.124535353);
				String edgeId = source +"_"+ destinations.get(i);
				Edge edge = graph.addEdge(edgeId, inV, outV, "created");
				edge.setProperty("epoch", epochs.get(i));
				}
				else{
					String edgeId = source +"_"+ destinations.get(i);
					Vertex inV = graph.getVertex(destinations.get(i).toString());
					Edge edge = graph.addEdge(edgeId, inV, outV, "created");
					edge.setProperty("epoch", epochs.get(i));
					
				}
				
			
			} // end inner for
		} // end outer for
			
			System.out.println(graph);
			GraphSONWriter.outputGraph(graph, new FileOutputStream("sample.json"));
	}

	public Map<String, List<List<String>>> getSubgraphWithTaskId(int taskId) {
		System.out.println("getSubgraphWithTaskId");
		try {
			Class.forName("com.vertica.jdbc.Driver");
			connection = DriverManager.getConnection("jdbc:vertica://16.25.152.150:5433/dbadmin", "dbadmin",
					"vertica");

			String query = "select source,destination,epoch_time from tvg4tm.ws_result where task_id="+taskId;
			System.out.println("getSubgraphWithTaskId:" + query);
			pstmt = connection.prepareStatement(query);
			resultset = pstmt.executeQuery();

			while (resultset.next()) {
				/**
				 * Store Subgraph in a map to create Json format
				 * 
				 */
				String source = String.valueOf(resultset.getLong("source"));
				String destination = String.valueOf(resultset.getLong("destination"));
				String epoch_time = String.valueOf(resultset.getLong("epoch_time"));
			//	System.out.println("S:" + source + " " + "D:" + destination + " " + "T:" + epoch_time);

				if (subGraph.containsKey(source)) {
					List<List<String>> mapValue = subGraph.get(source);
					List<String> destinationList = mapValue.get(0);
					List<String> epoch_timeList = mapValue.get(1);
					destinationList.add(destination);
					epoch_timeList.add(epoch_time);
					mapValue = new ArrayList<>();
					mapValue.add(destinationList);
					mapValue.add(epoch_timeList);
					// System.out.println("mapValue:"+mapValue);
					subGraph.put(source, mapValue);
				} else {
					List<String> destinationList = new ArrayList<String>();
					List<String> epoch_timeList = new ArrayList<String>();
					List<List<String>> mapValue = new ArrayList<>();
					destinationList.add(destination);
					epoch_timeList.add(epoch_time);
					mapValue.add(destinationList);
					mapValue.add(epoch_timeList);
					subGraph.put(source, mapValue);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (pstmt != null)
					pstmt.close();
				if (resultset != null)
					resultset.close();
				if (connection != null)
					connection.close();
			} catch (Exception ex) {

			}
		}
		return subGraph;
	}
	public static Set<String> getVertexList(Iterable<Vertex> iterable){
		Set<String> vertexSet = new HashSet<String>();
		Iterator<Vertex> iterator = iterable.iterator();
		while(iterator.hasNext()){
			Vertex v = iterator.next();
			System.out.println(v.getId());
			vertexSet.add(v.getId().toString());
		}
		System.out.println("Set:"+vertexSet);
		return vertexSet;
		
	}
	

}
