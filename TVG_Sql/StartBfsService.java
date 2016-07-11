package tvg.hpl.hp.com.ws;

import java.util.Properties;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hpl.hp.com.bean.StartBfsBean;
import com.hpl.hp.com.dao.GetSubGraph;
import com.hpl.hp.com.dao.StartBfsDao;
import com.hpl.hp.com.websocket.push.TVGWidget;

import tvg.hpl.hp.com.util.ManageAppProperties;

@Path("/StartBfs")
public class StartBfsService implements Runnable{
	static Logger log = LoggerFactory.getLogger(StartBfsService.class);
	private StartBfsBean queryBean;
	private StartBfsDao startbfsdao;

	public StartBfsService() {
		// TODO Auto-generated constructor stub
		
	}
	public StartBfsService(StartBfsBean queryBeanpara) {
		// TODO Auto-generated constructor stub
		queryBean = queryBeanpara;
	}
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public String  startBfs(@QueryParam("startTime") String startTime,@QueryParam("endTime") String endTime,
			@QueryParam("vertices") String vertices,@QueryParam("hop") String hop,@QueryParam("push_when_done") String push_when_done){
			
		if(startTime !=null && endTime !=null && vertices!=null && hop !=null && push_when_done !=null){
			queryBean = new StartBfsBean();			
			queryBean.setStartTime(startTime);
			queryBean.setEndTime(endTime);
			queryBean.setVertices(vertices);
			queryBean.setHop(hop);
			queryBean.setPush_when_done(push_when_done);
			Properties p = ManageAppProperties.getInstance().getApp_prop();
			System.out.println("Display property :"+p);
			/**
			 * Create Task Id and store Client Query
			 */
			startbfsdao = new StartBfsDao();
			queryBean = startbfsdao.createTaskId(queryBean);
			System.out.println("Return val :"+queryBean.getTaskId());
			
			/**
			 * Create a new Thread and get the Sub Graph
			 */
			StartBfsService runnable = new StartBfsService(queryBean);	
			Thread thread = new Thread(runnable);
			thread.start();
		}
		/**
		 * Return the taskId and status to the client
		 */
		JSONObject jsonobj = new JSONObject();

	     try {
			jsonobj.put("task_id", queryBean.getTaskId());
			jsonobj.put("status", queryBean.getStatus());
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	     
		return jsonobj.toString();
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		System.out.println("thread is running "+queryBean.getTaskId());
		System.out.println("Hop :"+ queryBean.getHop());
		//new GetSubGraph(queryBean).readTestTable();
		TVGWidget obj = new TVGWidget();
		String json = "{taskId:"+queryBean.getTaskId()+", status:"+queryBean.getStatus()+"}";
		obj.sendMessageOverSocket(json,queryBean.getTaskId());
		
	}

}
