package tvg.hpl.hp.com.ws;

/**
 * @author sarmaji
 *
 */

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import tvg.hpl.hp.com.bean.QueryResultBean;
import tvg.hpl.hp.com.bean.ResponseMessageBean;
import tvg.hpl.hp.com.bean.StartBfsBean;
import tvg.hpl.hp.com.dao.StartBfsDao;
import tvg.hpl.hp.com.services.GetSubGraph;

import tvg.hpl.hp.com.util.ApplicationConstants;
import tvg.hpl.hp.com.util.GraphsonUtil;
import tvg.hpl.hp.com.validation.StartBfsBeanValidation;
import tvg.hpl.hp.com.websocket.TVGWidget;

@Path("/StartBfs")
@Api(value="/StartBfs")
public class StartBfsService implements Runnable {
	static Logger log = LoggerFactory.getLogger(StartBfsService.class);
	private StartBfsBean startBfsBean;
	private QueryResultBean queryResultBean;
	private String taskId;
	private StartBfsDao startBfsDao;
	private String responseJsonMessage;
	private String responseJsonErrorMessage;
	private String jsonStringGraph;
	private Response response;

	public StartBfsService() {
	}

	/**
	 * 
	 * @param startBfsBeanPara
	 * @param taskIdPara
	 */

	public StartBfsService(StartBfsBean startBfsBeanPara, String taskIdPara) {
		startBfsBean = startBfsBeanPara;
		taskId = taskIdPara;
	}

	/**
	 * startbfs Web service takes user request and store the request in the
	 * database creates a taskId(tid) and returns the status and the taskID to
	 * the user. It's then extract the subgraph from the database and finding
	 * all the edges of the vertices and stores the result in the database. The
	 * computed subgraph then returns back to the client through the web socket
	 * based on the push_when_done_flag value. When push_when_done=0 return the
	 * results back to the client over web socket.
	 * 
	 * 
	 * @param startTime
	 * @param endTime
	 * @param vertices
	 * @param hop
	 * @param push_when_done
	 * @return
	 */

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.TEXT_PLAIN)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Ok"),
			@ApiResponse(code =400, message = "Error") })
	@ApiOperation(value = "Compute K-hop between start time and end time to a depth of hops starting from [vertices]."
			+ " The results in the form of a set of tuples (source vertex, target vertex, epoch, tid) will be stored in"
			+ " a single result table in Vertica. Each resulting graph will be indexed by its task id.")
	public Response startBfs(@ApiParam(required=true,value="The start of the selection time interval")@QueryParam("startTime") String startTime,
			@ApiParam(required=true,value="The end of the selection time interval")@QueryParam("endTime") String endTime,
			@ApiParam(required=true,value="A comma separated list of vertex ids to start BFS from")@QueryParam("vertices") String vertices, 
			@ApiParam(required=true,value="The depth of BFS search")@QueryParam("hop") String hop,
			@ApiParam(required=true,value=" A Boolean flag 0 or 1 ")@QueryParam("push_when_done") String push_when_done) {

		ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
		Validator validator = factory.getValidator();
		startBfsBean = new StartBfsBean();
	
		startBfsBean.setStartTime(startTime);
		startBfsBean.setEndTime(endTime);
		startBfsBean.setVertices(vertices);
		startBfsBean.setHop(hop);
		startBfsBean.setPushWhenDone(push_when_done);
		
		Set<ConstraintViolation<StartBfsBean>> constraintViolations = validator.validate(startBfsBean);
		log.error("StartBfs: Input Validation Error Size:" + constraintViolations.size());

		if (constraintViolations.size() > 0) {
			try {
				throw new StartBfsBeanValidation(constraintViolations);
			} catch (StartBfsBeanValidation e) {
				log.error("Error in StartBfsBeanValidation:" + e.getMessage());
			}
			/*response = Response.status(ApplicationConstants.RESPONSE_CODE_BAD_REQUEST).entity(responseJsonErrorMessage)
					.type(MediaType.APPLICATION_JSON).build();*/
		} 
		else {

			/**
			 * Create Task Id and store Client Query
			 */
			startBfsDao = new StartBfsDao();
			queryResultBean = startBfsDao.createTaskId(startBfsBean);
			log.info("Task Id Created :" + queryResultBean.getTaskId());

			if (queryResultBean != null && queryResultBean.getStatus().equalsIgnoreCase(ApplicationConstants.REQUEST_STATUS_RECEIVED)) {
				/**
				 * Create a new Thread and get the Sub Graph
				 */
				
					StartBfsService runnable = new StartBfsService(startBfsBean, queryResultBean.getTaskId());
					Thread thread = new Thread(runnable);
					thread.start();
			

				/**
				 * Return the taskId and status to the client in JSON format
				 * 
				 */
				ResponseMessageBean messageBean = new ResponseMessageBean();
				messageBean.setTid(queryResultBean.getTaskId());
				messageBean.setStatus(ApplicationConstants.RESPONSE_STATUS_SUCCESSFUL);
				GraphsonUtil graphSonutilObj = new GraphsonUtil();
				responseJsonMessage = graphSonutilObj.jsonResponseMessage(messageBean);

				/**
				 * Accept the request and send the response code 202(Accepted)
				 * along with the taskId and status.
				 * 
				 */
				response = Response.status(ApplicationConstants.RESPONSE_CODE_ACCEPTED).entity(responseJsonMessage)
						.type(MediaType.APPLICATION_JSON).build();
			} else {
				responseJsonErrorMessage = "internal Server error";
				response = Response.status(ApplicationConstants.RESPONSE_CODE_INTERNAL_SERVER_ERROR)
						.entity(responseJsonErrorMessage).type(MediaType.APPLICATION_JSON).build();
			}
		}
		return response;
	}

	@Override
	public void run() {
		/**
		 * Extract sub Graph and put it in the result table
		 */
		Map<String, List<List<String>>> subGraph = new HashMap<>();
		subGraph = new GetSubGraph(startBfsBean, taskId).executeQuery();
		
		log.info("SubGraph Size:" + subGraph.size());

		if (subGraph.size() > 0) {

			/**
			 * Update the user request status
			 * 
			 */
			log.info("Update TaskId Status in the Database");
			startBfsDao = new StartBfsDao();
			queryResultBean = startBfsDao.updateUserRequestStatusByTaskId(taskId);

			/**
			 * Pass the sub Graph to GraphsonUtil and convert it to the Json
			 * format
			 * 
			 */
			log.info("Construct Subgraph in JSON format");
			GraphsonUtil graphsonObj = new GraphsonUtil();
			jsonStringGraph=graphsonObj.subGraphJson(subGraph);
			System.out.println("Modified jsonstring :"+jsonStringGraph);

			/**
			 * Parse json object to Json String
			 * 
			 */
			//jsonStringGraph = graphsonObj.parseGraphsonFormat();
			//System.out.println("original jsonstring :"+jsonStringGraph);
		}

		/**
		 * if push_when_done =1 write results to the TVGwidget through WebSocket
		 */
		if (startBfsBean.getPushWhenDone().equals(ApplicationConstants.PUSH_WHEN_DONE_TRUE)) {
			log.info("Pushing SubGraph to TVGWIDGET through Web Socket");
			TVGWidget widgetObj = new TVGWidget();
			widgetObj.sendSubGraphOverSocket(jsonStringGraph);
		}
	}

}
