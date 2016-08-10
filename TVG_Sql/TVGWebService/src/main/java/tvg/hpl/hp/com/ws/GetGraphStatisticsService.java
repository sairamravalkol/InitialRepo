package tvg.hpl.hp.com.ws;


import java.util.List;
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
import javax.ws.rs.core.Response.Status;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.ApiResponse;
import tvg.hpl.hp.com.bean.GetGraphStatisticsBean;
import tvg.hpl.hp.com.bean.GraphStatisticsResultBean;
import tvg.hpl.hp.com.bean.QueryResultBean;
import tvg.hpl.hp.com.bean.ResponseErrorMessageBean;

import tvg.hpl.hp.com.bean.StartBfsBean;
import tvg.hpl.hp.com.dao.GetGraphStatisticsDao;
import tvg.hpl.hp.com.dao.ShowGraphDao;
import tvg.hpl.hp.com.util.ApplicationConstants;
import tvg.hpl.hp.com.util.GraphsonUtil;
import tvg.hpl.hp.com.validation.GetGraphStatisticsBeanValidation;



/**
 * The API will return different graph statistics that are TBD. In particular 
 * it will return the time range that the graph correspond to, the seed vertices 
 * and hop count. These should be stored as part of the handling of StartBFS. 
 * Additional statistics on a result graph (such as performance related statistics)
 *  might be required and will returned in a formatted JSON.
 * @author JIBAN SHARAMA JYOTI
 * @author SAIRAM RAVALKOL
 * @version 1.0.0 07/08/2016
 *
 */

@Path("/GetGraphStatistics")
@Api(value="/GetGraphStatistics")
public class GetGraphStatisticsService {
	static Logger log = LoggerFactory.getLogger(GetGraphStatisticsService.class);

	private StartBfsBean startBfsBean;
	private ShowGraphDao showGraphDao;
	private String responseJsonErrorMessage;
	private GetGraphStatisticsBean getGraphStatisticsBean;;
	private GetGraphStatisticsDao getGraphStatisticsDao;
	private Response response;
	private GraphStatisticsResultBean graphStatisticsResultBean;

	public GetGraphStatisticsService() {

	}

	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "The API will return different graph statistics that are TBD. In particular it will return the time range that the graph correspond to,"
			+ " the seed vertices and hop count. These should be stored as part of the handling of StartBFS. Additional statistics on a result graph (such as performance related statistics)"
			+ " might be required and will returned in a formatted JSON.")
	@ApiResponses(value = { @ApiResponse(code = 202, message = "Successful"),@ApiResponse(code =400, message = "Error on Input") })
	public Response getGraphStatistics(@ApiParam(required=true,name="tid",value="The id of a previously computed graph which resulted from a call to StartBFS")
										@QueryParam("tid") String taskId) {
		
		ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
		Validator validator = factory.getValidator();
		getGraphStatisticsBean = new GetGraphStatisticsBean();
		getGraphStatisticsBean.setTaskId(taskId);
		Set<ConstraintViolation<GetGraphStatisticsBean>> constraintViolations = validator.validate(getGraphStatisticsBean);
		log.error("Error in GetGraphStatistics Bean validation :" + constraintViolations.size());	
		
		if (constraintViolations.size() > 0) {
			try {
				throw new GetGraphStatisticsBeanValidation(constraintViolations);
			} catch (GetGraphStatisticsBeanValidation e) {
				log.error("GetGraphStatistics Bean validation:"+e.getMessage());
			}
			response = Response.status(ApplicationConstants.RESPONSE_CODE_BAD_REQUEST).entity(responseJsonErrorMessage).type(MediaType.APPLICATION_JSON).build();
		}		
		else {
			
			/**
			 * Checking status is completed or not
			 */
			showGraphDao = new ShowGraphDao();
			QueryResultBean queryResultBean = showGraphDao.checkTaskIdStatus(taskId);
			
			if (queryResultBean != null && queryResultBean.getStatus().equals(ApplicationConstants.REQUEST_STATUS_COMPLETED)) {
				getGraphStatisticsDao = new GetGraphStatisticsDao();
				List<Object> list = getGraphStatisticsDao.getGraphstatistics(taskId);
				startBfsBean = (StartBfsBean) list.get(0);
				graphStatisticsResultBean = (GraphStatisticsResultBean) list.get(1);

				/** putting GraphStatistics  properties into JSON Object */
				JSONObject json = new JSONObject();
				try {
					json.put("tid", taskId);
					json.put("num_vertices", graphStatisticsResultBean.getNum_vertices());
					json.put("num_edges", graphStatisticsResultBean.getNum_edges());
					json.put("start_time", startBfsBean.getStartTime());
					json.put("end_time", startBfsBean.getEndTime());
					json.put("depth", startBfsBean.getHop());
				} catch (JSONException e) {
					log.error("Error in creating JSON Object");
			}
				return Response.status(Status.ACCEPTED).entity(json.toString()).type(MediaType.APPLICATION_JSON)
						.build();
			} 
			else if(queryResultBean !=null){
				String statusMessage = null;
				if (queryResultBean.getStatus().equals(ApplicationConstants.REQUEST_STATUS_RECEIVED)) {
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
				System.out.println("Send response:"+statusMessage);
				response = Response.status(ApplicationConstants.RESPONSE_CODE_BAD_REQUEST).entity(responseJsonErrorMessage)
						.type(MediaType.APPLICATION_JSON).build();
			}
			else{
				responseJsonErrorMessage = "internal Server error";
				response = Response.status(ApplicationConstants.RESPONSE_CODE_INTERNAL_SERVER_ERROR)
						.entity(responseJsonErrorMessage).type(MediaType.APPLICATION_JSON).build();
			}
		} 
		return response;


		}

	}


