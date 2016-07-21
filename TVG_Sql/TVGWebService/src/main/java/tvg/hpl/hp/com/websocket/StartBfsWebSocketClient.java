package tvg.hpl.hp.com.websocket;

/**
 * @author sarmaji
 *
 */
import java.net.URI;

import javax.json.JsonObject;
import javax.websocket.ClientEndpoint;
import javax.websocket.CloseReason;
import javax.websocket.ContainerProvider;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.WebSocketContainer;

import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tvg.hpl.hp.com.ws.StartBfsService;

@ClientEndpoint
public class StartBfsWebSocketClient {
	static Logger log = LoggerFactory.getLogger(StartBfsWebSocketClient.class);
	Session userSession = null;
	public StartBfsWebSocketClient(URI serverEndpointURI) {
		try {
			log.info("3.");
			WebSocketContainer container = ContainerProvider.getWebSocketContainer();
			container.connectToServer(this, serverEndpointURI);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@OnOpen
	public void onOpen(Session userSession) {
		log.info("4 Client:Open Web Socket to the Server End Point ");
		this.userSession = userSession;
	}

	/**
	 * Callback hook for Connection close events.
	 *
	 * @param userSession
	 *            the userSession which is getting closed.
	 * @param reason
	 *            the reason for connection close
	 */
	@OnClose
	public void onClose(Session userSession, CloseReason reason) {
		log.info("client: closing websocket");
		this.userSession = null;
	}

	/**
	 * Callback hook for Message Events. This method will be invoked when a
	 * client send a message.
	 *
	 * @param message
	 *            The text message
	 */
	@OnMessage
	public void onMessage(String jsonsubGraph) {
		log.info("client: received message " + jsonsubGraph);
	}

	public void sendSubGraph(String jsonsubGraph) {
		log.info("client sendSubGraph:"+jsonsubGraph);
		try{
			
		this.userSession.getAsyncRemote().sendText(jsonsubGraph);
		}catch(Exception ex){
			log.error("Error sending message :"+ex.getMessage());
		}
	}
	/*@OnError
	public void onError(Session session, Throwable thr) {
		log.error("Error message :"+thr.getStackTrace().toString());
	}*/

}