package tvg.hpl.hp.com.ws;

import java.util.Properties;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tvg.hpl.hp.com.util.ManageAppProperties;

@Path("/DeleteGraph")
public class DeleteGraphService {
	static Logger log = LoggerFactory.getLogger(DeleteGraphService.class);
	
	public DeleteGraphService() {
		// TODO Auto-generated constructor stub
	}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public String  startBfs(@QueryParam("taskId") String taskId){
			
		if(taskId !=null){
			Properties p = ManageAppProperties.getInstance().getApp_prop();
			System.out.println("Delete Graph");
			
		}		
		return "{tid:1, status :received}";
	}
}
