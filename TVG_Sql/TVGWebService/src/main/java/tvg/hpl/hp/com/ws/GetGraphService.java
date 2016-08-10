package tvg.hpl.hp.com.ws;

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

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.ApiResponse;
import tvg.hpl.hp.com.bean.GetGraphBean;
import tvg.hpl.hp.com.bean.QueryResultBean;
import tvg.hpl.hp.com.bean.ResponseErrorMessageBean;
import tvg.hpl.hp.com.dao.ShowGraphDao;
import tvg.hpl.hp.com.services.ExtractSubGraph;
import tvg.hpl.hp.com.util.ApplicationConstants;
import tvg.hpl.hp.com.util.GraphsonUtil;
import tvg.hpl.hp.com.validation.GetGraphBeanValidation;

/**
 * The "GetGraphService"The API will instruct the web service to extract a
 * result graph, which is already stored in Vertica. The service will format the
 * results according to a GraphSON schema and return it to the caller.
 * 
 * @author JIBAN SHARMA
 * @author SAIRAM RAVALKOL
 * @version 1.0 27/07/2016
 */
@Path("/GetGraph")
@Api(value="/GetGraph")
public class GetGraphService {
	static Logger log = LoggerFactory.getLogger(GetGraphService.class);
	private GetGraphBean getGraphBean;
	private QueryResultBean queryResultBean;
	private ShowGraphDao showGraphDao;
	private String responseJsonErrorMessage;
	private String jsonStringGraph;
	private Response response;

	public GetGraphService() {
		
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
	@Consumes(MediaType.TEXT_PLAIN)
	@ApiResponses(value = { @ApiResponse(code=200, message = "Ok"),@ApiResponse(code = 400, message = "Error on Input") })
	@ApiOperation(value = "The API will instruct the web service to extract a result graph, "
			+ "which is already stored in Vertica. The service will format the results according to a GraphSON schema and return it to the caller.")
	public Response getGraph(@ApiParam(required=true,value="The id which identifies the invocation of a previously completed StartBFS")
							 @QueryParam("tid") String taskId) throws JSONException {

		ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
		Validator validator = factory.getValidator();
		getGraphBean = new GetGraphBean();
		getGraphBean.setTaskId(taskId);
		Set<ConstraintViolation<GetGraphBean>> constraintViolations = validator.validate(getGraphBean);
		log.error("GetGraph Input Validation error Size :" + constraintViolations.size());		
		if (constraintViolations.size() > 0) {
			try {
				throw new GetGraphBeanValidation(constraintViolations);
			} catch (GetGraphBeanValidation e) {
				log.error("GetGraph validation Error :"+e.getMessage());
			}
			response = Response.status(ApplicationConstants.RESPONSE_CODE_BAD_REQUEST).entity(responseJsonErrorMessage).type(MediaType.APPLICATION_JSON).build();
			
		} else {
				queryResultBean =new QueryResultBean();
				queryResultBean.setTaskId(taskId);
				showGraphDao = new ShowGraphDao();
				queryResultBean = showGraphDao.checkTaskIdStatus(taskId);
				
				/** checking whether taskId is completed or not. */
				if (queryResultBean !=null && queryResultBean.getStatus().equalsIgnoreCase(ApplicationConstants.REQUEST_STATUS_COMPLETED)) {
	
					Map<String, List<List<String>>> subGraph = new HashMap<>();
					ExtractSubGraph extractSubGraph = new ExtractSubGraph(queryResultBean.getTaskId());
					subGraph = extractSubGraph.getComputedSubGraph();
					System.out.println("Get SubGraph:" + subGraph);
					/** if subGraph is empty then throw exception */
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
						json.put("tid", queryResultBean.getTaskId());
						json.put("status", ApplicationConstants.RESPONSE_STATUS_SUCCESSFUL);
						json.put("result", graphobj);
						System.out.println(json.toString());
						/**
						 * Accept the request and send the response code
						 * 202(Accepted) along with the taskId and status
						 * 
						 */
						response = Response.status(ApplicationConstants.RESPONSE_CODE_OK).entity(json.toString()).type(MediaType.APPLICATION_JSON).build();
					} // end if
					else {
						/** Executed when Graph is Not available */
						ResponseErrorMessageBean errorMessageBean = new ResponseErrorMessageBean();
						errorMessageBean.setErrorMessage(" Graph is not available");
						GraphsonUtil graphson = new GraphsonUtil();
						responseJsonErrorMessage = graphson.jsonResponseErrorMessage(errorMessageBean);
						response = Response.status(ApplicationConstants.RESPONSE_CODE_BAD_REQUEST).entity(responseJsonErrorMessage).type(MediaType.APPLICATION_JSON)
								.build();
					}
				} else {
					String statusMessage = null;
					if (queryResultBean.getStatus().trim().equals(ApplicationConstants.REQUEST_STATUS_RECEIVED.trim())) {
						statusMessage = "computation is not yet completed";
					} 
					else if (queryResultBean.getStatus().equalsIgnoreCase(ApplicationConstants.REQUEST_STATUS_DELETED)) {
						statusMessage = "tid is already deleted";
					} 
					else {
						statusMessage = "tid not found";
					}
					ResponseErrorMessageBean errorMessageBean = new ResponseErrorMessageBean();
					errorMessageBean.setErrorMessage(statusMessage);
					GraphsonUtil graphsonUtil = new GraphsonUtil();
					responseJsonErrorMessage = graphsonUtil.jsonResponseErrorMessage(errorMessageBean);

					/**
					 * Return the status code 400(Bad Request) along with the status
					 * message
					 */
					response = Response.status(ApplicationConstants.RESPONSE_CODE_BAD_REQUEST).entity(responseJsonErrorMessage).type(MediaType.APPLICATION_JSON)
							.build();
				} // end else
			} // end outer else
		return response;
	}
}
