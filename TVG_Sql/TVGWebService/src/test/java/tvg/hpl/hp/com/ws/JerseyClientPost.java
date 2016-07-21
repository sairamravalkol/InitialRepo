package tvg.hpl.hp.com.ws;

import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class JerseyClientPost {
	private WebResource webResource;
	private ClientResponse response;
	private Client client;
	public static void main(String[] args) {

		try {

			Client client = Client.create();

			WebResource webResource = client
					.resource("http://localhost:8080/TVGWebService/api/startBFS");

		//	String input = "{\"singer\":\"Metallica\",\"title\":\"Fade To Black\"}";
			String input ="Jiban";

			ClientResponse response = webResource.type("application/json")
					.post(ClientResponse.class, input);

			if (response.getStatus() != 201) {
				throw new RuntimeException("Failed : HTTP error code : "
						+ response.getStatus());
			}

			System.out.println("Output from Server .... \n");
			String output = response.getEntity(String.class);
			System.out.println(output);

		} catch (Exception e) {

			e.printStackTrace();

		}
	new JerseyClientPost().test();;

	}
	public void test(){
		
		
		webResource = client.resource("http://localhost:8080/TVGWebService/api/startBfs/paramTVG")
				.queryParam("startTime", "43454465")
				.queryParam("endTime", "45346456").queryParam("hops", "1")
				.queryParam("vertices","12343,3456,5678");
		

		response = webResource.accept("application/json").type(MediaType.APPLICATION_JSON)
				.get(ClientResponse.class);
		System.out.println(response.toString());
		if (response.getStatus() == 200) {
		String	json_output_string = response.getEntity(String.class);
				System.out.println("First web service Call status:" + json_output_string);
		}

	}
}
