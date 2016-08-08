package tvg.hpl.hp.com.ws;

import java.util.List;
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

import tvg.hpl.hp.com.bean.ResponseErrorMessageBean;
import tvg.hpl.hp.com.bean.StartBfsBean;
import tvg.hpl.hp.com.bean.StatisticsBean;
import tvg.hpl.hp.com.dao.GetGraphStatisticsDao;
import tvg.hpl.hp.com.dao.ShowGraphDao;
import tvg.hpl.hp.com.util.GraphsonUtil;

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
public class GetGraphStatisticsService {
	static Logger log = LoggerFactory.getLogger(GetGraphStatisticsService.class);
	private StartBfsBean queryBean;
	private ShowGraphDao showGraphDao;
	private String responseJsonErrorMessage;
	private StatisticsBean statBean;
	private GetGraphStatisticsDao getGraphStatisticsDao;

	public GetGraphStatisticsService() {
		// TODO Auto-generated constructor stub

	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getGraphStatistics(@QueryParam("tid") String taskId) {
		if (taskId.matches("^[0-9]*$") && taskId != null && !taskId.equals("null") && !taskId.isEmpty()) {
			queryBean = new StartBfsBean();
			queryBean.setTaskId(taskId);
			/**
			 * Checking status is completed or not
			 */
			showGraphDao = new ShowGraphDao();
			StartBfsBean checkTaskIdStatus = showGraphDao.checkTaskIdStatus(queryBean);
			String status = checkTaskIdStatus.getStatus();
			// System.out.println(status!=null);
			if (status != null && checkTaskIdStatus.getStatus().equals("completed")) {
				getGraphStatisticsDao = new GetGraphStatisticsDao();
				List<Object> list = getGraphStatisticsDao.fetch(checkTaskIdStatus);
				queryBean = (StartBfsBean) list.get(0);
				statBean = (StatisticsBean) list.get(1);

				/** putting GraphStatistics  properties into JSON Object */
				JSONObject json = new JSONObject();
				try {
					json.put("tid", queryBean.getTaskId());
					json.put("num_vertices", statBean.getNum_vertices());
					json.put("num_edges", statBean.getNum_edges());
					json.put("start_time", queryBean.getStartTime());
					json.put("end_time", queryBean.getEndTime());
					json.put("depth", queryBean.getHop());
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				return Response.status(Status.ACCEPTED).entity(json.toString()).type(MediaType.APPLICATION_JSON)
						.build();

			} else {

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
		} else {
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
