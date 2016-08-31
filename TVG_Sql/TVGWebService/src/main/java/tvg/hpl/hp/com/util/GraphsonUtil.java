package tvg.hpl.hp.com.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONWriter;

import tvg.hpl.hp.com.bean.ResponseErrorMessageBean;
import tvg.hpl.hp.com.bean.ResponseMessageBean;
import tvg.hpl.hp.com.graph.json.GraphEntity;
import tvg.hpl.hp.com.services.GetNodeMetaData;

/**
 * @author sarmaji
 *
 */
public class GraphsonUtil {
	static Logger log = LoggerFactory.getLogger(GraphsonUtil.class);
	private String responseJsonMessage;
	private String responseJsonErrorMessage;
	private String jsonStringGraph;
	private Map<String, List<String>> nodeMetaDataMap = new HashMap<>();

	public String subGraphJson(Map<String, List<List<String>>> subGraphMap) {

		Graph graph = new TinkerGraph();
		nodeMetaDataMap = GetNodeMetaData.getNodeMetaData();
		try {

			for (Map.Entry<String, List<List<String>>> subgraph : subGraphMap.entrySet()) {
				String source = subgraph.getKey();
				Iterable<Vertex> iterable1 = graph.getVertices();
				Set<String> vertexSet1 = this.getVertexList(iterable1);
				Vertex outV = null;

				if (!vertexSet1.contains(source)) {
					List<String> nodeMetaDataList = nodeMetaDataMap.get(source);
					outV = graph.addVertex(source);
					if (nodeMetaDataMap.containsKey(source)) {
						outV.setProperty("black_list", nodeMetaDataList.get(2));
						outV.setProperty("external_flag", nodeMetaDataList.get(0));
						outV.setProperty("machine_type", nodeMetaDataList.get(1));
						outV.setProperty("page_rank", 0);
					}
					else {
						outV.setProperty("black_list", "NA");
						outV.setProperty("external_flag", "NA");
						outV.setProperty("machine_type", "NA");
						outV.setProperty("page_rank", 0);	
						log.error("NodeMetaData Doesn't contain meta info for vertex:" + source);
					}
				} else {
					outV = graph.getVertex(source.toString());
				}
				List<List<String>> list = subgraph.getValue();
				List<String> destinations = list.get(0);
				List<String> epochs = list.get(1);

				for (int i = 0; i < destinations.size(); i++) {
					Iterable<Vertex> iterable = graph.getVertices();
					Set<String> vertexSet = this.getVertexList(iterable);
					if (!vertexSet.contains(destinations.get(i))) {
						Vertex inV = graph.addVertex(destinations.get(i));
						if (nodeMetaDataMap.containsKey(destinations.get(i))) {
						List<String> nodeMetaDataList = nodeMetaDataMap.get(destinations.get(i));
					
							inV.setProperty("black_list", nodeMetaDataList.get(2));
							inV.setProperty("external_flag", nodeMetaDataList.get(0));
							inV.setProperty("machine_type", nodeMetaDataList.get(1));
							inV.setProperty("page_rank", 0);
							String edgeId = source + "_" + destinations.get(i);
							Edge edge = graph.addEdge(edgeId, inV, outV, "created");
							edge.setProperty("epoch", epochs.get(i));
						}
						else{
							inV.setProperty("black_list", "NA");
							inV.setProperty("external_flag", "NA");
							inV.setProperty("machine_type", "NA");
							inV.setProperty("page_rank", 0);
							String edgeId = source + "_" + destinations.get(i);
							Edge edge = graph.addEdge(edgeId, inV, outV, "created");
							edge.setProperty("epoch", epochs.get(i));
						log.error("NodeMetaData Doesn't contain meta info for vertex:" + destinations.get(i));
						}
					} else {
						String edgeId = source + "_" + destinations.get(i);
						Vertex inV = graph.getVertex(destinations.get(i).toString());
						Edge edge = graph.addEdge(edgeId, inV, outV, "created");
						edge.setProperty("epoch", epochs.get(i));
					}
				} // end inner for
			} // end outer for
		} catch (Exception ex) {
			log.error("Error getting meta Data info :" + ex.getMessage());

		}
		/*try {
			ManageAppProperties instance = ManageAppProperties.getInstance();
			Properties property = instance.getApp_prop();
			String dirPath = property.getProperty(ApplicationConstants.GRAPHSON_JSON_PATH);
			Path path = Paths.get(dirPath);
			if (!Files.exists(path)) {
				try {
					Files.createDirectories(path);
				} catch (IOException e) {
					log.error("Error creating Directory for Json files");
				}
			}
			StringBuilder file = new StringBuilder();
			file.append(dirPath).append("graph.json");
			File jsonFile = new File(file.toString());
			if (!jsonFile.exists()) {
				jsonFile.createNewFile();
			}
			GraphSONWriter.outputGraph(graph, new FileOutputStream(jsonFile));
		} catch (FileNotFoundException e) {
			log.error("Error writing graph to json file in subGraphJson:" + e.getMessage());
		} catch (IOException e) {
			log.error("Error in subGraphJson :" + e.getMessage());
		}*/
		Iterable<Vertex> vertexIterable = graph.getVertices();
		Iterator<Vertex> vertexIterator = vertexIterable.iterator();
		List<JSONObject> jsonVertexList = new ArrayList<>(); 
		JSONObject json;
		while (vertexIterator.hasNext()) {
			Vertex v = vertexIterator.next();
			try {
				json = GraphSONUtility.jsonFromElement(v, null, GraphSONMode.NORMAL);
				jsonVertexList.add(json);
			//	System.out.println(json);
			} catch (JSONException e) {
				log.error("Error in converting vertex from Graphson to JSON :"+e.getMessage());
			}		
		}
		
		Iterable<Edge> edgeIterable = graph.getEdges();
		Iterator<Edge> edgeIterator = edgeIterable.iterator();
		List<JSONObject> jsonEdgeList = new ArrayList<>(); 
	
		while (edgeIterator.hasNext()) {
			Edge e= edgeIterator.next();
			try {
				json = GraphSONUtility.jsonFromElement(e, null, GraphSONMode.NORMAL);
				jsonEdgeList.add(json);				
			} catch (JSONException e1) {
				log.error("Error in converting edge from Graphson to JSON :"+e1.getMessage());
			}		
		}
		
		JSONObject jsonGraph=new JSONObject();
		JSONObject outputGraph = new JSONObject();
		try {
			jsonGraph.put("mode", GraphSONMode.NORMAL);
			jsonGraph.put("vertices", jsonVertexList);
			jsonGraph.put("edges", jsonEdgeList);
			outputGraph.put("graph", jsonGraph);
			System.out.println(outputGraph);

		} catch (JSONException e) {
			log.error("Error in constructing the Graph in JSON format :"+e.getMessage());
		}
		return outputGraph.toString();		
	}

	/*public String parseGraphsonFormat() {
		System.out.println("parseGraphsonFormat");
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			*//**
			 * Convert JSON string from file to Object
			 *//*
			ManageAppProperties instance = ManageAppProperties.getInstance();
			Properties property = instance.getApp_prop();
			String dirPath = property.getProperty(ApplicationConstants.GRAPHSON_JSON_PATH);
			String jsonFile = dirPath + "graph.json";
			File file = new File(jsonFile);
			GraphEntity graphEntity = objectMapper.readValue(file, GraphEntity.class);
			tvg.hpl.hp.com.graph.json.Graph graphObj = new tvg.hpl.hp.com.graph.json.Graph();
			graphObj.setGraph(graphEntity);
			jsonStringGraph = objectMapper.writeValueAsString(graphObj);
			//System.out.println("new jsonStringGraph:" + jsonStringGraph);
		//	log.info("size of jsonStringGraph in Bytes:" + jsonStringGraph.getBytes().length);
		} catch (IOException ex) {
			log.error("Error in parsing in parseGraphsonFormat:" + ex.getMessage());
		}
		return jsonStringGraph;
	}
*/
	public Set<String> getVertexList(Iterable<Vertex> iterable) {
		Set<String> vertexSet = new HashSet<String>();
		Iterator<Vertex> iterator = iterable.iterator();

		while (iterator.hasNext()) {
			Vertex v = iterator.next();
			vertexSet.add(v.getId().toString());
		}
		return vertexSet;
	}

	public String jsonResponseMessage(ResponseMessageBean message) {
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			responseJsonMessage = objectMapper.writeValueAsString(message);
			// System.out.println("responseJsonMessage:"+responseJsonMessage);
		} catch (JsonProcessingException e) {
			log.error("Error in parsing json in jsonResponseMessage:" + e.getMessage());
		}
		return responseJsonMessage;
	}

	public String jsonResponseErrorMessage(ResponseErrorMessageBean errorMessage) {
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			responseJsonErrorMessage = objectMapper.writeValueAsString(errorMessage);
			// System.out.println("responseJsonMessage:"+responseJsonMessage);
		} catch (JsonProcessingException e) {
			log.error("Json processing error in jsonResponseErrorMessage:" + e.getMessage());
		}
		return responseJsonErrorMessage;
	}

}
