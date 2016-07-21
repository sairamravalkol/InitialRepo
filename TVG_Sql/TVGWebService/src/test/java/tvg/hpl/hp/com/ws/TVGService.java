package tvg.hpl.hp.com.ws;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.dbcp2.BasicDataSource;

import tvg.hpl.hp.com.util.DataBaseUtility;

@Path("/startBfs1")
public class TVGService {

	public TVGService() {
		// TODO Auto-generated constructor stub
	}
	
	@POST
	@Path("post")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response createTrackInJSON(String track) {
		System.out.println("Received:"+track);

		String result = "Track saved : " + track;
		return Response.status(201).entity(result).build();
		
	}
	@GET
	@Path("/paramTVG")
	@Produces(MediaType.APPLICATION_JSON)
	public TestResults getQueryParamInJSON(@QueryParam("vertices") String vertices,
	    @QueryParam("startTime") String starttime, @QueryParam("endTime") String endtime,
	    @QueryParam("hops") String hops) {
		
		System.out.println("tvggetqueryparam");
		TestResults result = new TestResults();
		result.setVertices(vertices);
		result.setStarttime(starttime);
		result.setEndtime(endtime);
		result.setHops(hops);
		BasicDataSource datasource = DataBaseUtility.getVerticaDataSource();
		try {
			Connection connection = datasource.getConnection();
			PreparedStatement ps = connection.prepareStatement("select * from tvg4tm.dns_data_regression_test limit 10");
			ResultSet rs = ps.executeQuery();
			while(rs.next()){
				System.out.println(rs.getLong("source"));
			}
			System.out.println(connection.toString());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("connection error");
			e.printStackTrace();
		}
		

		return result;
		
	}
	@GET
	@Produces(MediaType.TEXT_PLAIN)
    public String getIt(@QueryParam("name") String name) {
		System.out.println("Get name :"+name);
        return "Got it!"+name;
    }
	
	

	
}
