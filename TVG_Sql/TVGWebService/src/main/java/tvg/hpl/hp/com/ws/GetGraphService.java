package tvg.hpl.hp.com.ws;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import tvg.hpl.hp.com.bean.StartBfsBean;
import tvg.hpl.hp.com.dao.ShowGraphDao;
import tvg.hpl.hp.com.dao.StartBfsDao;
import tvg.hpl.hp.com.util.GraphsonUtil;

/**
 * The "GetGraphService"The API will instruct the web service 
 * to extract a result graph, which is already stored in Vertica.
 *  The service will format the results according to a GraphSON 
 *  schema and return it to the caller.
 * 
 * @author JIBAN SHARMA JYOTHI
 * @author SAIRAM RAVALKOL
 * @version 1.0 27/07/2016
 */
@Path("/GetGraph")
public class GetGraphService {
	static Logger log = LoggerFactory.getLogger(GetGraphService.class);
	private StartBfsBean queryBean;
	private ShowGraphDao showGraphDao;
	private String responseJsonErrorMessage;
	private String jsonStringGraph;

	public GetGraphService() {
		// TODO Auto-generated constructor stub

	}

	/**
	 * "getGraph" method performs fetching all resulted graph from Vertica which
	 * is specified by taskId with taskId and Status code.
	 * 
	 * @param taskId, 
	 *            the id which identifies the invocation of a previously
	 *            completed StartBFS.
	 * @return Response : The response is a JSON with an indication on the task
	 *         id, status report and the result graph.
	 * @throws JSONException
	 */
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getGraph(@QueryParam("tid") String taskId) throws JSONException {

		/** Input validation with notNull & is Empty and allow only numeric */
		if (taskId.matches("^[0-9]*$") && taskId != null && !taskId.equals("null") && !taskId.isEmpty()) {
			queryBean = new StartBfsBean();
			queryBean.setTaskId(taskId);

			showGraphDao = new ShowGraphDao();
			queryBean = showGraphDao.checkTaskIdStatus(queryBean);
			/** checking whether taskId is completed or not.*/
			if (queryBean.getStatus().equals("completed")) {

				Map<String, List<List<String>>> subGraph = new HashMap<>();
				StartBfsDao bfsDao = new StartBfsDao();
				subGraph = bfsDao.getSubgraphWithTaskId(queryBean);
				// System.out.println("SubGraph : "+subGraph);
				System.out.println("taskId ::" + queryBean.getTaskId());

				System.out.println("print:" + subGraph);
				/** if subGraph is empty then throw exception*/
				if (subGraph.size() > 0) {

					GraphsonUtil graphsonObj = new GraphsonUtil();
					graphsonObj.subGraphJson(subGraph);

					/**
					 * Parse json object to Json String
					 * 
					 */
					jsonStringGraph = graphsonObj.parseGraphsonFormat();

					// response.setResult(jsonStringGraph);
					JSONObject graphobj = new JSONObject(jsonStringGraph);
					
					System.out.println(graphobj.toString());
					JSONObject json = new JSONObject();
					json.put("taskId", queryBean.getTaskId());
					json.put("Status", queryBean.getStatus());
					json.put("result", graphobj);
					System.out.println(json.toString());
					 /**
					 * Accept the request and send the response code 202(Accepted) along
					 * with the taskId and status
					 * 
					 */

					return Response.status(202).entity(json.toString()).type(MediaType.APPLICATION_JSON).build();

				} // end if
				else {
					/** Executed when Graph is Not available */
					ResponseErrorMessageBean errorMessageBean = new ResponseErrorMessageBean();
					errorMessageBean.setErrorMessage(" Graph is not available");
					GraphsonUtil graphson = new GraphsonUtil();
					responseJsonErrorMessage = graphson.jsonResponseErrorMessage(errorMessageBean);
					return Response.status(400).entity(responseJsonErrorMessage).type(MediaType.APPLICATION_JSON)
							.build();
				}

			} else {
				/** Executes when status is not "Completed".*/
				ResponseErrorMessageBean errorMessageBean = new ResponseErrorMessageBean();
				errorMessageBean.setErrorMessage("Status : Not completed");
				GraphsonUtil graphsonUtil = new GraphsonUtil();
				responseJsonErrorMessage = graphsonUtil.jsonResponseErrorMessage(errorMessageBean);

				/**
				 * Return the status code 400(Bad Request) along with the status
				 * message
				 */
				return Response.status(400).entity(responseJsonErrorMessage).type(MediaType.APPLICATION_JSON).build();

			} // end inner else
		} // end outer if
		else {
			/** execute When user enters Invalid task id */
			ResponseErrorMessageBean errorMessageBean = new ResponseErrorMessageBean();
			errorMessageBean.setErrorMessage("invalid parameter");
			GraphsonUtil graphsonUtil = new GraphsonUtil();
			responseJsonErrorMessage = graphsonUtil.jsonResponseErrorMessage(errorMessageBean);

			/**
			 * Return the status code 400(Bad Request) along with the status
			 * message
			 */
			return Response.status(400).entity(responseJsonErrorMessage).type(MediaType.APPLICATION_JSON).build();

		} // end outer else

	}

}
