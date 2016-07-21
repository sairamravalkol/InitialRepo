package tvg.hpl.hp.com.ws;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbcp2.BasicDataSource;
import org.eclipse.persistence.internal.jaxb.many.MapValue;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;

public class GetResults {
	private BasicDataSource datasource;
	private Connection connection;
	private PreparedStatement pstmt;
	private Statement stmt;
	private ResultSet resultset;
	Map<String, List<List<String>>> subGraph = new HashMap<>();
	
	
	
	public Map<String, List<List<String>>> getSubgraphWithTaskId(int taskId) {
		try {
			Class.forName("com.vertica.jdbc.Driver");
			connection = DriverManager.getConnection("jdbc:vertica://16.25.152.150:5433/dbadmin", "dbadmin","vertica");
			String query = "select source,destination,epoch_time from tvg4tm.ws_result where task_id="+ taskId;
			System.out.println("getSubgraphWithTaskId:"+query);
			pstmt = connection.prepareStatement(query);
			resultset = pstmt.executeQuery();

			while (resultset.next()) {				
				String source = String.valueOf(resultset.getLong("source"));
				String destination = String.valueOf(resultset.getLong("destination"));
				String epoch_time = String.valueOf(resultset.getLong("epoch_time"));
				
				System.out.println("S:"+source + " "+"D:"+destination + " "+ "T:"+epoch_time);
				
			if (subGraph.containsKey(source)) {
				
				List<List<String>> mapValue = subGraph.get(source);
				//System.out.println("List : "+list);
				List<String> destinationList = mapValue.get(0);
				List<String> epoch_timeList = mapValue.get(1);
				destinationList.add(destination);
				epoch_timeList.add(epoch_time);
				mapValue = new ArrayList<>();
				mapValue.add(destinationList);
				mapValue.add(epoch_timeList);
				System.out.println("Destination List :"+destinationList);
			//	System.out.println("mapValue:"+mapValue);
				subGraph.put(source, mapValue);
			}  else {
				
				List<String> destinationList = new ArrayList<String>();
				List<String> epoch_timeList = new ArrayList<String>();
				List<List<String>> mapValue = new ArrayList<>();
				destinationList.add(destination);
				epoch_timeList.add(epoch_time);
				mapValue.add(destinationList);
				mapValue.add(epoch_timeList);
			//	System.out.println("mapValue:"+mapValue);
				subGraph.put(source, mapValue);
			 }
		 }
	/*		Graph graph = new TinkerGraph();
			for(Map.Entry<String, List<List<String>>> ll :subGraph.entrySet()){
				System.out.println("ff");				
				 List<List<String>> l = ll.getValue();
				 List<String> destination = l.get(0);
				 List<String> time = l.get(1);
				 String source = ll.getKey();		
				 Vertex source1 = graph.addVertex(source);
				 for(int i = 0; i<destination.size(); i++){
					
				Iterable<Vertex> iterable = graph.getVertices();
				if(!this.check(iterable,destination.get(i))){
					
					 Vertex destination1 = graph.addVertex(destination.get(i));
					 source1.setProperty("name", "marko");
					 destination1.setProperty("name", "peter");
					 Edge e = graph.addEdge(source+"_"+destination.get(i), source1, destination1, "knows");
				}
			}
				
		}*/
			System.out.println(subGraph);
			
	   } catch (Exception ex) {
		   ex.printStackTrace();
	  }
	  return subGraph;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		new GetResults().getSubgraphWithTaskId(7);
		
	/*	List<String> destinationList = new ArrayList<String>();
		List<String> epoch_timeList = new ArrayList<String>();
		List<List<String>> mapValue1 = new ArrayList<>();
		Map<String, List<List<String>>> graph = new HashMap<>();
		
		destinationList.add("1");
		destinationList.add("2");
		mapValue1.add(destinationList);
		
		graph.put("100", mapValue1);
		
	//	System.out.println(graph);
		
		if(graph.containsKey("100")){
			List<List<String>> mapValue2 =	graph.get("100");
			List<String> dest = mapValue2.get(0);
		//	System.out.println(dest);
			dest.add("3");
			dest.add("4");
			mapValue2.add(dest);
			graph.put("100", mapValue2);
			//System.out.println(dest);
			
			
		} */
		//System.out.println(graph);
		//new GetResults().graphSon();
	}
	
	public boolean check(Iterable iterable,String id){
		
		Iterator<Vertex> iter = iterable.iterator();
		
		while(iter.hasNext()){
			System.out.println();
			System.out.println(id);
			String id1= iter.next().getId().toString();
			if(id1.equals(id))
				System.out.println("true");
			return true;
		}
		return false;
			
		}

	
	public void graphSon(){
		Graph graph = new TinkerGraph();
		Vertex a = graph.addVertex("22");
		Vertex b = graph.addVertex("21");
		a.setProperty("name", "marko");
		b.setProperty("name", "peter");
		Edge e = graph.addEdge(null, a, b, "knows");
		System.out.println(e.getVertex(Direction.OUT).getProperty("name") + 
				"--" + e.getLabel() + "-->" + e.getVertex(Direction.IN).getProperty("name"));
		
		System.out.println(graph.getVertex("21").getId());
	}

}
