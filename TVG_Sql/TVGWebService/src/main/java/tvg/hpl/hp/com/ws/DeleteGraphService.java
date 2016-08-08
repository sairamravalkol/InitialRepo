package tvg.hpl.hp.com.ws;

import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tvg.hpl.hp.com.bean.DeleteGraphBean;
import tvg.hpl.hp.com.bean.QueryResultBean;
import tvg.hpl.hp.com.bean.ResponseErrorMessageBean;
import tvg.hpl.hp.com.bean.ResponseMessageBean;
import tvg.hpl.hp.com.dao.DeleteGraphDao;
import tvg.hpl.hp.com.dao.ShowGraphDao;
import tvg.hpl.hp.com.util.ApplicationConstants;
import tvg.hpl.hp.com.util.GraphsonUtil;
import tvg.hpl.hp.com.validation.DeleteGraphBeanValidation;

/**
 * The API will instruct the web service to ERASE (result) graph, which is
 * already stored in Vertica. the function will erase the graph that it attached
 * to tid.
 * 
 * @author JIBAN SHARMA
 * @author SAIRAM RAVALKOL
 * @version 1.0 22/07/2016
 */
@Path("/DeleteGraph")
public class DeleteGraphService {
	static Logger log = LoggerFactory.getLogger(DeleteGraphService.class);
	private QueryResultBean queryResultBean;
	private DeleteGraphBean deleteGraphBean;
	private ShowGraphDao showGraphDao;
	private DeleteGraphDao deleteGraphDao;
	private String responseJsonMessage;
	private String responseJsonErrorMessage;
	private Response response;

	public DeleteGraphService() {
	}

	/**
	 * the function will erase the graph that it attached to tid.
	 * 
	 * @param taskId,
	 *            tid - task id. The id of a previously computed graph which
	 *            resulted from a call to StartBFS.
	 * @return Return success or failed json response to the caller.
	 */
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response deleteGraph(@QueryParam("tid") String taskId) {

		ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
		Validator validator = factory.getValidator();
		deleteGraphBean = new DeleteGraphBean();
		deleteGraphBean.setTaskId(taskId);
		Set<ConstraintViolation<DeleteGraphBean>> constraintViolations = validator.validate(deleteGraphBean);
		log.error("Bean Error Size:" + constraintViolations.size());
		if (constraintViolations.size() > 0) {
			try {
				throw new DeleteGraphBeanValidation(constraintViolations);
			} catch (DeleteGraphBeanValidation e) {
				log.error("DeleteGraphService validation error :"+e.getMessage());
			}
			response = Response.status(ApplicationConstants.RESPONSE_CODE_BAD_REQUEST).entity(responseJsonErrorMessage).type(MediaType.APPLICATION_JSON).build();
		} 
		else {
				queryResultBean = new QueryResultBean();
				/**
				 * Checking status is completed or not
				 */
				showGraphDao = new ShowGraphDao();
				queryResultBean = showGraphDao.checkTaskIdStatus(taskId);
			if (queryResultBean !=null && queryResultBean.getStatus().equalsIgnoreCase(ApplicationConstants.REQUEST_STATUS_COMPLETED)) {
				deleteGraphDao = new DeleteGraphDao();
				queryResultBean = deleteGraphDao.deleteSubGRaphByTaskId(queryResultBean.getTaskId());
				/**
				 * Update the user request status
				 * 
				 */
				log.info("Update TaskId Status in the Database");
				queryResultBean = deleteGraphDao.updateTokenDeleteStatusByTaskId(taskId);
				ResponseMessageBean message = new ResponseMessageBean();
				message.setTid(queryResultBean.getTaskId());
				message.setStatus(queryResultBean.getStatus());
				GraphsonUtil graphSonutilObj = new GraphsonUtil();
				responseJsonMessage = graphSonutilObj.jsonResponseMessage(message);
				response = Response.status(ApplicationConstants.RESPONSE_CODE_OK).entity(responseJsonMessage).type(MediaType.APPLICATION_JSON).build();

			} // end if
			else {
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
		} // end else
		return response;
	}// end deleteGraph
} // end DeleteGraphService
