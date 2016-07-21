package tvg.hpl.hp.com.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONWriter;

import tvg.hpl.hp.com.bean.ResponseErrorMessageBean;
import tvg.hpl.hp.com.bean.ResponseMessageBean;
import tvg.hpl.hp.com.dao.StartBfsDao;
import tvg.hpl.hp.com.graph.json.GraphEntity;

/**
 * @author sarmaji
 *
 */
public class GraphsonUtil {
	static Logger log = LoggerFactory.getLogger(GraphsonUtil.class);
	private String responseJsonMessage;
	private String responseJsonErrorMessage;
	private String jsonStringGraph;

	public void subGraphJson(Map<String, List<List<String>>> subGraphMap) {
		
		// TODO Auto-generated method stub
		Graph graph = new TinkerGraph();
		
		//	System.out.println(testGraph);
			Set<Entry<String,List<List<String>>>> entrySet = subGraphMap.entrySet();
		//	System.out.println(entrySet);
			
			for(Map.Entry<String, List<List<String>>> subgraph : subGraphMap.entrySet()) {
				
				String source = subgraph.getKey();
				Iterable<Vertex> iterable1 = graph.getVertices();
				Set<String> vertexSet1 = this.getVertexList(iterable1);
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
				
				
					
			//	System.out.println("destination size:"+destinations.size());
				for (int i=0; i< destinations.size(); i++) {
				//	System.out.println("source:"+source +" " + "des: "+destinations.get(i)+ " "+ "time:"+epochs.get(i));
					
					Iterable<Vertex> iterable = graph.getVertices();
					Set<String> vertexSet = this.getVertexList(iterable);
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
				
			//	System.out.println(graph);
				try {
					GraphSONWriter.outputGraph(graph, new FileOutputStream("C:\\WorkspaceTVG\\TVGWebService\\sample.json"));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
	}
	
	public String jsonResponseMessage(ResponseMessageBean message){
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			responseJsonMessage = objectMapper.writeValueAsString(message);
			//System.out.println("responseJsonMessage:"+responseJsonMessage);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return responseJsonMessage;
	}
	
	public String jsonResponseErrorMessage(ResponseErrorMessageBean errorMessage){
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			responseJsonErrorMessage = objectMapper.writeValueAsString(errorMessage);
			//System.out.println("responseJsonMessage:"+responseJsonMessage);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return responseJsonErrorMessage;
	}
	
	public String parseGraphsonFormat(){
		System.out.println("parseGraphsonFormat");
		ObjectMapper objectMapper = new ObjectMapper();

		try {			// Convert JSON string from file to Object
			GraphEntity graphEntity = objectMapper.readValue(new File("C:\\WorkspaceTVG\\TVGWebService\\sample.json"), GraphEntity.class);
			tvg.hpl.hp.com.graph.json.Graph graphObj = new tvg.hpl.hp.com.graph.json.Graph();
			graphObj.setGraph(graphEntity);
			jsonStringGraph = objectMapper.writeValueAsString(graphObj);
			System.out.println("new jsonStringGraph:"+jsonStringGraph);
			log.info("size of jsonStringGraph in Bytes:"+jsonStringGraph.getBytes().length);
		}catch(Exception ex){
			ex.printStackTrace();
		}
		return jsonStringGraph;
	}
	public static Set<String> getVertexList(Iterable<Vertex> iterable){
		Set<String> vertexSet = new HashSet<String>();
		Iterator<Vertex> iterator = iterable.iterator();
		while(iterator.hasNext()){
			Vertex v = iterator.next();
		//	System.out.println(v.getId());
			vertexSet.add(v.getId().toString());
		}
	//	System.out.println("Set:"+vertexSet);
		return vertexSet;
		
	}	

}
