package tvg.hpl.hp.com.ws;

import java.util.Properties;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tvg.hpl.hp.com.bean.StartBfsBean;
import tvg.hpl.hp.com.dao.GetSubGraph;
import tvg.hpl.hp.com.dao.StartBfsDao;
import tvg.hpl.hp.com.util.ManageAppProperties;
import tvg.hpl.hp.com.websocket.TVGWidget;

@Path("/GetGraphStatistics")
public class GetGraphStatisticsService {
	static Logger log = LoggerFactory.getLogger(GetGraphStatisticsService.class);
		
	public GetGraphStatisticsService() {
		// TODO Auto-generated constructor stub
		
	}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public String  startBfs(@QueryParam("taskId") String taskId){
		
		if(taskId !=null){						
			Properties p = ManageAppProperties.getInstance().getApp_prop();
			System.out.println("Get Graph Statistics");
		}		
		return "{tid:1, status :received}";
	}
	
}
