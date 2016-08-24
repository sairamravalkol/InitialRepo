
/* ================== 				SERVER INTERACTIONS 				================== */

/* ================== API (Web Socket) related functions ================== */

function initWebSocket()
{
	if ("WebSocket" in window)
	{
	   console.log("WS: WebSocket is supported by your Browser!");
	   
	   // Open a web socket
	   //var ws = new WebSocket(webSocketURL);
	  var ws = new WebSocket('ws://localhost:8080/TVGWebService/startBfs'); 
		
	   ws.onopen = function()
	   {
		  // Web Socket is connected, send data using send()
		  ws.send("Message to send");
		  console.log("WS: Message is sent.");
		  showActiveWSstatus();
	   };
		
	   ws.onmessage = function (evt) 
	   { 
		 var received_msg = evt.data;
		 console.log("WS: Message is received. Response is: ");
		 showActiveWSstatus();
		 if (IsJsonString(evt.data))
		 {
			var received_obj = $.parseJSON(evt.data);
		     console.log(received_obj);	
			if (received_obj.graph !== undefined)
			{
				if (renderMethod === "webGL")
				{
				// use webGL - canvas is much faster
				drawGraphWebGl(received_obj.graph);
				}
				else
				{
				// use SVG
				// drawGraph(received_obj.graph);
				   drawGraphLabels(received_obj.graph);
				}
			}			
		 }
		 else
		 {
			console.log("awaiting web socket data");
		 }
	   };
		
	   ws.onclose = function()
	   { 
		  // Web Socket is closed.
		  console.log("WS: Connection is closed."); 
		  showInactiveWSstatus();
	   };
	   
	   ws.onerror = function(evt)
	   { 
		  // Web Socket error.
		  console.log("WS: Web Socket error:"); 
		  console.log(evt);
		  showInactiveWSstatus();
	   };
	   
	}
	else
	{
	   // The browser doesn't support WebSocket
	   console.log("WS: WebSocket NOT supported by your Browser!");
	}
}

/* ================== API (REST) related functions ================== */

function sendRequest(type, endpointURL, data, callback) {
    // $("#spinner").show();
	//requestInActiveMode = true;
    $.ajax({
        url: endpointURL,
        type: type,
        //cache: false,
        dataType: 'json',
        xhrFields: {withCredentials: true},
        data: data,
        success: function (data, textStatus, xhr) {
          //  $("#spinner").hide();
            callback(data);

        },
        error: function (xhr, textStatus, errorThrown) {
            alert('Error ' + textStatus + ': ' + errorThrown);
        }
    });
}