package tvg.hpl.hp.com.util;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hpl.hp.com.bean.ResponseErrorMessageBean;
import com.hpl.hp.com.bean.ResponseMessageBean;

/**
 * @author sarmaji
 *
 */
public class GraphsonUtil {
	private String responseJsonMessage;
	private String responseJsonErrorMessage;

	public void subGraphJson(Map<String, List<List<String>>> subGraphMap) {
		// TODO Auto-generated method stub
		for(Map.Entry<String, List<List<String>>> subgraph : subGraphMap.entrySet()) {
			String source = subgraph.getKey();
			List<List<String>> list = subgraph.getValue();
			List<String> destinationList = list.get(0);
			List<String> epoch_time = list.get(1);
		//	System.out.println("source :"+source + " "+ "destination :"+destinationList);
			
			/*for(String destination : destinationList){
				System.out.println("Edge Id :"+source+"_"+destination);
			}*/
 			
		}		
	}
	
	public String jsonResponseMessage(ResponseMessageBean message){
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			responseJsonMessage = objectMapper.writeValueAsString(message);
			//System.out.println("responseJsonMessage:"+responseJsonMessage);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return responseJsonMessage;
	}
	
	public String jsonResponseErrorMessage(ResponseErrorMessageBean errorMessage){
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			responseJsonErrorMessage = objectMapper.writeValueAsString(errorMessage);
			//System.out.println("responseJsonMessage:"+responseJsonMessage);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return responseJsonErrorMessage;
	
		
	}

}
