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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tvg.hpl.hp.com.bean.ResponseErrorMessageBean;
import tvg.hpl.hp.com.bean.ResponseMessageBean;
import tvg.hpl.hp.com.bean.StartBfsBean;
import tvg.hpl.hp.com.dao.ShowGraphDao;
import tvg.hpl.hp.com.dao.StartBfsDao;
import tvg.hpl.hp.com.util.GraphsonUtil;
import tvg.hpl.hp.com.websocket.TVGWidget;

/**
 * 
 * @author Sairam Ravalkol
 * @version 1.0 22/07/2016
 */
@Path("/ShowGraph")
public class ShowGraphService implements Runnable	{
	static Logger log = LoggerFactory.getLogger(ShowGraphService.class);
	private StartBfsBean queryBean;
	private ShowGraphDao showGraphDao;
	private StartBfsDao startbfsdao;
	private String responseJsonMessage;
	private String responseJsonErrorMessage;
	private String jsonStringGraph;
	
	public ShowGraphService() {
		// TODO Auto-generated constructor stub	
	}
	
	public ShowGraphService(StartBfsBean querybeanpara) {
		
		this.queryBean = querybeanpara;
	}
	
	/**
	 * Method "showGraph" will send the response to client. As Success is 0 
	 * @param taskId
	 * @return 
	 */
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response showGraph(@QueryParam("tid") String taskId) {
	
		
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
			
				ResponseMessageBean response = new ResponseMessageBean();
				response.setTask_id(checkTaskIdStatus.getTaskId());
				response.setStatus("0");
				
				ShowGraphService runnable = new ShowGraphService(checkTaskIdStatus);
				System.out.println(checkTaskIdStatus.getPush_when_done());
				//System.out.println(queryBean.getTaskId()+ " "+queryBean.getStatus());
				Thread thread = new Thread(runnable);
				thread.run();
				

				GraphsonUtil graphsonUtilObj = new GraphsonUtil();
				
			    responseJsonMessage = graphsonUtilObj.jsonResponseMessage(response);
			   
			   /**
				 * Accept the request and send the response code 202(Accepted) along
				 * with the taskId and status
				 * 
				 */
				
				return Response.status(202).entity(responseJsonMessage).build();
			}
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
		@Override
		public void run() {
			// TODO Auto-generated method stub
						
			Map<String, List<List<String>>> subGraph = new HashMap<>();
			StartBfsDao bfsDao = new StartBfsDao();
			subGraph = bfsDao.getSubgraphWithTaskId(queryBean);
			//System.out.println("SubGraph : "+subGraph);
			System.out.println("taskId ::" + queryBean.getTaskId());

			System.out.println("print:" + subGraph);

			/**
			 * Update the user request status
			 * 
			 */
			startbfsdao = new StartBfsDao();
			queryBean = startbfsdao.updateUserRequestStatus(queryBean);

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
				jsonStringGraph=graphsonObj.parseGraphsonFormat();
				/**
				 * write results to the TVGwidget through WebSocket
				 */
				TVGWidget widgetObj = new TVGWidget();
				widgetObj.sendSubGraphOverSocket(jsonStringGraph);
				
			}

	}
}
