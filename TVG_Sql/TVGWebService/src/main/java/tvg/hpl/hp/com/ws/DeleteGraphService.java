package tvg.hpl.hp.com.ws;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tvg.hpl.hp.com.bean.ResponseErrorMessageBean;
import tvg.hpl.hp.com.bean.ResponseMessageBean;
import tvg.hpl.hp.com.bean.StartBfsBean;
import tvg.hpl.hp.com.dao.DeleteGraphDao;
import tvg.hpl.hp.com.dao.ShowGraphDao;
import tvg.hpl.hp.com.util.GraphsonUtil;

/**
 * The API will instruct the web service to ERASE (result) graph, 
 * which is already stored in Vertica.
 * the function will erase the graph that it attached to tid.
 * @author JIBAN SHARMA
 * @author SAIRAM RAVALKOL
 * @version 1.0 22/07/2016
 */
@Path("/DeleteGraph")
public class DeleteGraphService {
	static Logger log = LoggerFactory.getLogger(DeleteGraphService.class);

	private StartBfsBean queryBean;
	private ShowGraphDao showGraphDao;
	private DeleteGraphDao deleteGraphDao;
	private String responseJsonMessage;
	private String responseJsonErrorMessage;
	public DeleteGraphService() {
		// TODO Auto-generated constructor stub
	}
	
	/**
	 * the function will erase the graph that it attached to tid.
	 * @param taskId, tid - task id. The id of a previously computed
	 *  graph which resulted from a call to StartBFS.
	 * @return Return success or failed json response to the caller.
	 */
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response  deleteGraph(@QueryParam("tid") String taskId) {
		
		if(taskId.matches("^[0-9]*$") && taskId!=null && !taskId.equals("null") && !taskId.isEmpty())
		{
			queryBean = new StartBfsBean();
			queryBean.setTaskId(taskId);
			/**
			 *  Checking status is completed or not
			 */
			showGraphDao = new ShowGraphDao();
			StartBfsBean checkTaskIdStatus = showGraphDao.checkTaskIdStatus(queryBean);

			if(checkTaskIdStatus.getStatus().equals("completed")) 
			{
				 deleteGraphDao= new DeleteGraphDao();
				 StartBfsBean delete = deleteGraphDao.delete(checkTaskIdStatus);
				 ResponseMessageBean message = new ResponseMessageBean();
					message.setTask_id(delete.getTaskId());
					message.setStatus(delete.getStatus());
					GraphsonUtil graphSonutilObj = new GraphsonUtil();
					responseJsonMessage = graphSonutilObj.jsonResponseMessage(message);

					/**
					 * Accept the request and send the response code 202(Accepted) along
					 * with the taskId and status
					 * 
					 */

					return Response.status(202).entity(responseJsonMessage).type(MediaType.APPLICATION_JSON).build();

			} // end if
			else
			{

				ResponseErrorMessageBean errorMessageBean = new ResponseErrorMessageBean();
				errorMessageBean.setErrorMessage("Status : Not Completed");
				GraphsonUtil graphsonUtil = new GraphsonUtil();
				responseJsonErrorMessage = graphsonUtil.jsonResponseErrorMessage(errorMessageBean);

				/**
				 * Return the status code 400(Bad Request) along with the status
				 * message
				 */
				return Response.status(400).entity(responseJsonErrorMessage).type(MediaType.APPLICATION_JSON).build();
			} // end else

		}// end if
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

		} // end else


	}// end deleteGraph
} // end DeleteGraphService
