package tvg.hpl.hp.com.ws;

/**
 * @author sarmaji
 *
 */

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hpl.hp.com.bean.ResponseErrorMessageBean;
import com.hpl.hp.com.bean.ResponseMessageBean;
import com.hpl.hp.com.bean.StartBfsBean;
import com.hpl.hp.com.dao.GetSubGraph;
import com.hpl.hp.com.dao.StartBfsDao;
import com.hpl.hp.com.websocket.push.TVGWidget;
import tvg.hpl.hp.com.util.GraphsonUtil;

@Path("/StartBfs")
public class StartBfsService implements Runnable {
	static Logger log = LoggerFactory.getLogger(StartBfsService.class);
	private StartBfsBean queryBean;
	private StartBfsDao startbfsdao;
	private String responseJsonMessage;
	private String responseJsonErrorMessage;

	public StartBfsService() {
		// TODO Auto-generated constructor stub

	}

	public StartBfsService(StartBfsBean queryBeanpara) {
		// TODO Auto-generated constructor stub
		queryBean = queryBeanpara;
	}

	// Bean Validation is required
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response startBfs(@QueryParam("startTime") @NotNull String startTime,
			@QueryParam("endTime") @NotNull String endTime, @QueryParam("vertices") @NotNull String vertices,
			@QueryParam("hop") @NotNull String hop, @QueryParam("push_when_done") @NotNull String push_when_done) {

		if (!startTime.isEmpty() && !endTime.isEmpty() && !vertices.isEmpty() && !hop.isEmpty()
				&& !push_when_done.isEmpty()) {
			queryBean = new StartBfsBean();
			queryBean.setStartTime(startTime);
			queryBean.setEndTime(endTime);
			queryBean.setVertices(vertices);
			queryBean.setHop(hop);
			queryBean.setPush_when_done(push_when_done);

			/**
			 * Create Task Id and store Client Query
			 */
			startbfsdao = new StartBfsDao();
			queryBean = startbfsdao.createTaskId(queryBean);
			System.out.println("Return val :" + queryBean.getTaskId());

			/**
			 * Create a new Thread and get the Sub Graph
			 */
			StartBfsService runnable = new StartBfsService(queryBean);
			Thread thread = new Thread(runnable);
			thread.start();

			/**
			 * Return the taskId and status to the client in JSON format
			 * 
			 */
			ResponseMessageBean message = new ResponseMessageBean();
			message.setTask_id(queryBean.getTaskId());
			message.setStatus(queryBean.getStatus());
			GraphsonUtil graphSonutilObj = new GraphsonUtil();
			responseJsonMessage = graphSonutilObj.jsonResponseMessage(message);

			/**
			 * Accept the request and send the response code 202(Accepted) along
			 * with the taskId and status
			 * 
			 */

			return Response.status(202).entity(responseJsonMessage).type(MediaType.APPLICATION_JSON).build();

		} else {
			ResponseErrorMessageBean errorMessageBean = new ResponseErrorMessageBean();
			errorMessageBean.setErrorMessage("invalid parameters");
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
		System.out.println("Hop :" + queryBean.getHop());

		/**
		 * Extract sub Graph and put it in the result table
		 */
		Map<String, List<List<String>>> subGraph = new HashMap<>();
		subGraph = new GetSubGraph(queryBean).executeQuery();

		System.out.println("print:" + subGraph);

		/**
		 * Update the user request status
		 * 
		 */
		startbfsdao = new StartBfsDao();
		queryBean = startbfsdao.updateUserRequestStatus(queryBean);

		/**
		 * Pass the sub Graph to GraphsonUtil and convert it to the Json format
		 * 
		 */
		if (subGraph.size() > 0) {
			GraphsonUtil graphsonObj = new GraphsonUtil();
			graphsonObj.subGraphJson(subGraph);
		}

		/**
		 * if push_when_done =1 write results to the TVGwidget through WebSocket
		 */
		if (queryBean.getPush_when_done().equals("1")) {
			TVGWidget obj = new TVGWidget();
			String json = "{taskId:" + queryBean.getTaskId() + ", status:" + queryBean.getStatus() + "}";
			obj.sendMessageOverSocket(json, queryBean.getTaskId());
		}
		// else

	}

}
