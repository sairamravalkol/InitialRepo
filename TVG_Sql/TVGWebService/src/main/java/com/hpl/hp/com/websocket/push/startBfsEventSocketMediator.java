package com.hpl.hp.com.websocket.push;

/**
 * @author sarmaji
 *
 */
import java.io.IOException;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

@ServerEndpoint("/startBfs")
public class startBfsEventSocketMediator {

	private static Set<Session> peers = Collections.synchronizedSet(new HashSet<Session>());

	@OnMessage
	public String onMessage(String message, Session session, @PathParam("client-id") String clientId) {
		try {
			JSONObject jObj = new JSONObject(message);
			System.out.println("received message from client " + clientId);
			System.out.println("Peers size:" + peers.size());
			for (Session s : peers) {
				try {

					Map<String, String> map = s.getPathParameters();
					//if (map.get("client-id").equals(clientId)) {
						System.out.println("map value :" + map.get("client-id"));
						System.out.println("map val :" + map.values());
						System.out.println("session id :" + s.getId());
						System.out.println("Path para:" + s.getPathParameters());
						System.out.println("Request para:" + s.getRequestParameterMap());
						s.getBasicRemote().sendText(message);
					/*} else {
						String errmessage = "You are not the right client";
						s.getBasicRemote().sendText(errmessage);
					}*/
					System.out.println("send message to peer ");
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return "message was received by socket mediator and processed: " + message;
	}

	@OnOpen
	public void onOpen(Session session, @PathParam("client-id") String clientId) {
		System.out.println("mediator: opened websocket channel for client " + clientId);
		System.out.println(session.getPathParameters());
		System.out.println(session.getRequestParameterMap());
		peers.add(session);
		System.out.println("Perrs size in websocket :" + peers.size());

		try {
			session.getBasicRemote().sendText("good to be in touch jiban");
		} catch (IOException e) {
		}
	}

	@OnClose
	public void onClose(Session session, @PathParam("client-id") String clientId) {
		System.out.println("mediator: closed websocket channel for client " + clientId);
		peers.remove(session);
	}
}