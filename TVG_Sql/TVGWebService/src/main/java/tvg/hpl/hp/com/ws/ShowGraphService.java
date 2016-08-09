package tvg.hpl.hp.com.ws;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

import tvg.hpl.hp.com.bean.QueryResultBean;
import tvg.hpl.hp.com.bean.ResponseErrorMessageBean;
import tvg.hpl.hp.com.bean.ResponseMessageBean;
import tvg.hpl.hp.com.bean.ShowGraphBean;
import tvg.hpl.hp.com.dao.ShowGraphDao;
import tvg.hpl.hp.com.services.ExtractSubGraph;
import tvg.hpl.hp.com.util.ApplicationConstants;
import tvg.hpl.hp.com.util.GraphsonUtil;
import tvg.hpl.hp.com.validation.ShowGraphBeanValidation;
import tvg.hpl.hp.com.websocket.TVGWidget;

/**
 * The API will instruct the web service to send a new (result) graph, which is
 * already stored in Vertica, to the attached TVG widget. The service will
 * format the results according to a GraphSON schema,
 *
 * @author JIBAN SHARMA
 * @author SAIRAM RAVALKOL
 * @version 1.0 22/07/2016
 */
@Path("/ShowGraph")
public class ShowGraphService implements Runnable {
	static Logger log = LoggerFactory.getLogger(ShowGraphService.class);
	private ShowGraphBean showGraphBean;
	private QueryResultBean queryResultBean;
	private ShowGraphDao showGraphDao;
	private String responseJsonMessage;
	private String responseJsonErrorMessage;
	private String jsonStringGraph;
	private Response response;

	public ShowGraphService() {
	}

	public ShowGraphService(QueryResultBean responseBeanPara) {
		queryResultBean = responseBeanPara;
	}

	/**
	 * Method "showGraph" will send the response to client. As Success is 0
	 * 
	 * @param taskId
	 * @return
	 */
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response showGraph(@QueryParam("tid") String taskId) {

		ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
		Validator validator = factory.getValidator();
		showGraphBean = new ShowGraphBean();
		showGraphBean.setTId(taskId);
		Set<ConstraintViolation<ShowGraphBean>> constraintViolations = validator.validate(showGraphBean);
		log.error("ShowGraph Validation Error Size:" + constraintViolations.size());

		if (constraintViolations.size() > 0) {
			try {
				throw new ShowGraphBeanValidation(constraintViolations);
			} catch (ShowGraphBeanValidation e) {
				log.error(e.getMessage());
			}
			response = Response.status(ApplicationConstants.RESPONSE_CODE_BAD_REQUEST).entity(responseJsonErrorMessage)
					.type(MediaType.APPLICATION_JSON).build();
		} 
		else {
				queryResultBean = new QueryResultBean();
				/**
				 * Checking status is completed or not
				 */
				showGraphDao = new ShowGraphDao();
				queryResultBean = showGraphDao.checkTaskIdStatus(taskId);

			if (queryResultBean !=null && queryResultBean.getStatus().equalsIgnoreCase(ApplicationConstants.REQUEST_STATUS_COMPLETED)) {
				ShowGraphService runnable = new ShowGraphService(queryResultBean);
				Thread thread = new Thread(runnable);
				thread.start();
				ResponseMessageBean messageBean = new ResponseMessageBean();
				messageBean.setTid(queryResultBean.getTaskId());
				messageBean.setStatus(ApplicationConstants.RESPONSE_STATUS_SUCCESSFUL);
				GraphsonUtil graphsonUtilObj = new GraphsonUtil();
				responseJsonMessage = graphsonUtilObj.jsonResponseMessage(messageBean);
				System.out.println("Message:"+responseJsonMessage);
				/**
				 * Accept the request and send the response code 202(Accepted)
				 * along with the taskId and status
				 * 
				 */
				response = Response.status(ApplicationConstants.RESPONSE_CODE_ACCEPTED).entity(responseJsonMessage)
						.type(MediaType.APPLICATION_JSON).build();
			} 
			else if(queryResultBean !=null){
				System.out.println("not comepleted:" + queryResultBean.getStatus());
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

	@Override
	public void run() {

		Map<String, List<List<String>>> subGraph = new HashMap<>();
		ExtractSubGraph extractSubGraph = new ExtractSubGraph(queryResultBean.getTaskId());
		subGraph = extractSubGraph.getComputedSubGraph();
		// System.out.println("SubGraph : "+subGraph);
		System.out.println("taskId ::" + queryResultBean.getTaskId());
		System.out.println("ShowGraph:" + subGraph);
		/**
		 * Pass the sub Graph to GraphsonUtil and convert it to the JSON format.
		 * 
		 */
		if (subGraph.size() > 0) {
			GraphsonUtil graphsonObj = new GraphsonUtil();
			graphsonObj.subGraphJson(subGraph);

			/**
			 * Parse json object to Json String
			 * 
			 */
			jsonStringGraph = graphsonObj.parseGraphsonFormat();
			/**
			 * write results to the TVGwidget through WebSocket
			 */
			TVGWidget widgetObj = new TVGWidget();
			widgetObj.sendSubGraphOverSocket(jsonStringGraph);
		}
		else{
			log.error("Error Extracting SubGraph");
		}
	}
}
