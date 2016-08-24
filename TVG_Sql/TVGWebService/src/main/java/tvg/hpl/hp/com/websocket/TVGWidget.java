package tvg.hpl.hp.com.websocket;

/**
 * @author sarmaji
 *
 */
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tvg.hpl.hp.com.util.ApplicationConstants;
import tvg.hpl.hp.com.util.ManageAppProperties;

public class TVGWidget {
	static Logger log = LoggerFactory.getLogger(TVGWidget.class);
	private StartBfsWebSocketClient webSocketClient;
	private String webSocketAddress; 
	
	public TVGWidget(){
		try{
			ManageAppProperties instance = ManageAppProperties.getInstance();
			Properties property = instance.getApp_prop();
			String host_name = property.getProperty(ApplicationConstants.WEB_SOCKET_HOST_NAME);
			String port = property.getProperty(ApplicationConstants.WEB_SOCKET_PORT);
			StringBuilder sb = new StringBuilder();
			sb.append("ws").append(":").append("//").append(host_name).append(":")
			.append(port).append("/TVGWebService").append("/startBfsWs");
			webSocketAddress=sb.toString();		
			log.info("1. WebsocketAddress:"+webSocketAddress);
		}catch(Exception ex){
			log.error("Error in Initilization of the TVGWidget() Constructor:"+ex.getMessage()); 
		}
	}

	private void initializeWebSocket() throws URISyntaxException {
		log.info("2.Open websocket client at " + webSocketAddress);
		webSocketClient = new StartBfsWebSocketClient(new URI(webSocketAddress));
	}

	public void sendSubGraphOverSocket(String jsonsubGraph) {
		if (webSocketClient == null) {
			try {
					initializeWebSocket();
				} catch (URISyntaxException e) {
				log.error("Error URI Couldn't be Parsed in sendSubGraphOverSocket:"+e.getMessage());
			}catch (RuntimeException e) {
				log.error("WebSocket Runtime Exception:"+e.getMessage());
			}
		}
		webSocketClient.sendSubGraph(jsonsubGraph);
	}
}
