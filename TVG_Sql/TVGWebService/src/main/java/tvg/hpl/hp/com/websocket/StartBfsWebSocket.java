package tvg.hpl.hp.com.websocket;

/**
 * @author sarmaji
 *
 */
import java.io.IOException;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.json.JsonObject;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tvg.hpl.hp.com.ws.StartBfsService;

@ServerEndpoint("/startBfsWs")
public class StartBfsWebSocket {
	static Logger log = LoggerFactory.getLogger(StartBfsWebSocket.class);
	private static Set<Session> clients = Collections.synchronizedSet(new HashSet<Session>());

	@OnMessage
	public String onMessage(String jsonsubGraph, Session session) {
		log.info("6. Server recevied SubGraph:"+jsonsubGraph);
		log.info("Number of Clients Connected:" + clients.size());
			for (Session s : clients) {
				try {
					s.getBasicRemote().sendText(jsonsubGraph);
					log.info("Server:send Subgraph to the Client");
					System.out.println("send subGraph to the client ");
				} catch (IOException e) {
					log.error("Error in sending Subgraph from server to the Client:"+e.getMessage());
			}
		}
		return "message was received by the server socket and processed: " + jsonsubGraph;
	}

	@OnOpen
	public void onOpen(Session session) {
		log.info("5.WebSocket Server : opened websocket channel for client ");
		clients.add(session);
		try {
			session.getBasicRemote().sendText("Connected to the WebSocket Server");
		} catch (IOException e) {
			log.error("Error in Opening WebSocket Session for Client in onOpen(Session session) method.");
		}
	}

	@OnClose
	public void onClose(Session session) {
		System.out.println("WebSocket Server: closed websocket channel for client ");
		clients.remove(session);
	}
	@OnError
	public void onError(Session session, Throwable thr) {
		log.error("Error message :"+thr.getStackTrace().toString());
		clients.remove(session);
	}
}