package tvg.hpl.hp.com.ws.client;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class WebServiceClient {

	public WebServiceClient() {
		// TODO Auto-generated constructor stub
	}
	
	public void testGetMethod(){
		try{
		Client client = Client.create();
		/*WebResource webresource = client.resource("http://localhost:8080/TVGWebService/api/startBfs/")
				.queryParam("startTime", "43454465")
				.queryParam("endTime", "45346456").queryParam("hops", "1")
				.queryParam("vertices","12343,3456,5678");
		ClientResponse response = webresource.accept("application/json").get(ClientResponse.class);*/
		String taskId = "344";
		String name = "jib";
		WebResource webresource = client.resource("http://localhost:8080/TVGWebService/api/GetGraphStatistics")
				.queryParam("taskId", taskId).queryParam("name", name);				
		ClientResponse response = webresource.accept("application/json").get(ClientResponse.class);
	//	System.out.println("Response status:"+response.getStatus());
		if(response.getStatus()==200){
			String output = response.getEntity(String.class);
			System.out.println("Success:"+output);
		}
		else{
			String output1 = response.getEntity(String.class);
			System.out.println("fail:"+output1);
		}
		}catch (Exception e) {
			// TODO: handle exception
		//	System.out.println("Error:"+e.getMessage());
		}
	}
	
	public static void main(String[] args){
		WebServiceClient obj = new WebServiceClient();
		obj.testGetMethod();
		
	}
	
	

}
