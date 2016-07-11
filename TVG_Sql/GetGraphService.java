package tvg.hpl.hp.com.ws;

import java.util.Properties;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hpl.hp.com.bean.StartBfsBean;
import com.hpl.hp.com.dao.GetSubGraph;
import com.hpl.hp.com.dao.StartBfsDao;
import com.hpl.hp.com.websocket.push.TVGWidget;

import tvg.hpl.hp.com.util.ManageAppProperties;

@Path("/GetGraph")
public class GetGraphService {
	static Logger log = LoggerFactory.getLogger(GetGraphService.class);
	
	public GetGraphService() {
		// TODO Auto-generated constructor stub
		
	}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public String  startBfs(@QueryParam("taskId") String taskId){
		
		if(taskId !=null){						
			Properties p = ManageAppProperties.getInstance().getApp_prop();
			System.out.println("Get Graph Service");
			
			/**
			 * First check the status of the Taskid In the user_query table if its completed 
			 * query the result table and return the results else return the taskid and the status
			 */
			
		}		
		return "{tid:1, status :received}";
	}
	

}
