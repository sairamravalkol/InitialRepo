package tvg.hpl.hp.com.ws;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tvg.hpl.hp.com.bean.ResponseErrorMessageBean;
import tvg.hpl.hp.com.bean.ResponseMessageBean;
import tvg.hpl.hp.com.bean.StartBfsBean;
import tvg.hpl.hp.com.dao.ShowGraphDao;
import tvg.hpl.hp.com.dao.StartBfsDao;
import tvg.hpl.hp.com.util.GraphsonUtil;

@Path("/GetGraph")
public class GetGraphService {
	static Logger log = LoggerFactory.getLogger(GetGraphService.class);
	private StartBfsBean queryBean;
	private ShowGraphDao showGraphDao;
	private StartBfsDao startbfsdao;
	private String responseJsonMessage;
	private String responseJsonErrorMessage;
	private String jsonStringGraph;
	
	public GetGraphService() {
		// TODO Auto-generated constructor stub
		
	}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getGraph(@QueryParam("tid")String taskId) throws JSONException {
		
		if(taskId.matches("^[0-9]*$") && taskId!=null && !taskId.equals("null") && !taskId.isEmpty()) {
			queryBean = new StartBfsBean();
			queryBean.setTaskId(taskId);

			showGraphDao = new ShowGraphDao();
			 queryBean = showGraphDao.checkTaskIdStatus(queryBean);
			 
			 if(queryBean.getStatus().equals("completed")) {
				JsonObject jsonObjectBuilder = Json.createObjectBuilder().build();
				jsonObjectBuilder.put("taskId", queryBean.getTaskId());
				jsonObjectBuilder.put("Status", "0");
				
				Map<String, List<List<String>>> subGraph = new HashMap<>();
				StartBfsDao bfsDao = new StartBfsDao();
				subGraph = bfsDao.getSubgraphWithTaskId(queryBean);
				//System.out.println("SubGraph : "+subGraph);
				System.out.println("taskId ::" + queryBean.getTaskId());

				System.out.println("print:" + subGraph);
				if (subGraph.size() > 0) {
					
					GraphsonUtil graphsonObj = new GraphsonUtil();
					graphsonObj.subGraphJson(subGraph);
					
					
					/**
					 * Parse json object to Json String
					 * 
					 */
					jsonStringGraph=graphsonObj.parseGraphsonFormat();
					jsonObjectBuilder.add("result", jsonStringGraph);
					System.out.println(jsonObjectBuilder);
					//response.setResult(jsonStringGraph);
					
				   
				 
				   
				   return Response.status(202).entity(null).type(MediaType.APPLICATION_JSON).build();
				
				}
				else
				{
					ResponseErrorMessageBean errorMessageBean = new ResponseErrorMessageBean();
					errorMessageBean.setErrorMessage(" Graph is not available");
					GraphsonUtil graphson = new GraphsonUtil();
					responseJsonErrorMessage = graphson.jsonResponseErrorMessage(errorMessageBean);
					return Response.status(400).entity(responseJsonErrorMessage).type(MediaType.APPLICATION_JSON).build();
				}
					 
				
			 }
			 else
			 {
				 ResponseErrorMessageBean errorMessageBean = new ResponseErrorMessageBean();
					errorMessageBean.setErrorMessage("Status : completed");
					GraphsonUtil graphsonUtil = new GraphsonUtil();
					responseJsonErrorMessage = graphsonUtil.jsonResponseErrorMessage(errorMessageBean);

					/**
					 * Return the status code 400(Bad Request) along with the status
					 * message
					 */
					return Response.status(400).entity(responseJsonErrorMessage).type(MediaType.APPLICATION_JSON).build();
					
 
			 }
		} 
		else
		{
			ResponseErrorMessageBean errorMessageBean = new ResponseErrorMessageBean();
			errorMessageBean.setErrorMessage("invalid parameter");
			GraphsonUtil graphsonUtil = new GraphsonUtil();
			responseJsonErrorMessage = graphsonUtil.jsonResponseErrorMessage(errorMessageBean);

			/**
			 * Return the status code 400(Bad Request) along with the status
			 * message
			 */
			return Response.status(400).entity(responseJsonErrorMessage).type(MediaType.APPLICATION_JSON).build();
			
		}
		
	}

}
