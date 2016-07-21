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
import tvg.hpl.hp.com.ws.StartBfsService;

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
			ex.printStackTrace();
		}
	}

	private void initializeWebSocket() throws URISyntaxException {
		//System.out.println("REST service: open websocket client at " + webSocketAddress);
		log.info("2.REST service: open websocket client at " + webSocketAddress);
		webSocketClient = new StartBfsWebSocketClient(new URI(webSocketAddress));
	}

	public void sendSubGraphOverSocket(String jsonsubGraph) {
		if (webSocketClient == null) {
			try {
					initializeWebSocket();
				} catch (URISyntaxException e) {
				e.printStackTrace();
			}
		}
		webSocketClient.sendSubGraph(jsonsubGraph);
	}
}
