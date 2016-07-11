package com.hpl.hp.com.websocket.push;

import java.net.URI;
import java.net.URISyntaxException;

public class TVGWidget {
	

	private StartBfsEventSocketClient client;

	private final String webSocketAddress = "ws://localhost:8080/TVGWebService/startBfs";

	private void initializeWebSocket(String taskId) throws URISyntaxException {
			System.out.println("REST service: open websocket client at " + webSocketAddress);
		client = new StartBfsEventSocketClient(new URI(webSocketAddress + "/"+taskId));
	}

	public void sendMessageOverSocket(String message,String taskId) {
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (client == null) {
			try {
				initializeWebSocket(taskId);
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
		}
		client.sendMessage(message);

	}

}
