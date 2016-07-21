function drawGraphLabels(graphData)
{
	console.log("Function drawGraph: Creating nodes and edges from current batch");
	// ===== Graph Initialization	======== //
	if (graph == null) 
	{
		// create a graph object.
		graph = Viva.Graph.graph();
	}
	else 
	{
		$("#VGcontainer").empty();
		// clear last data
		graph.clear();
		graph = Viva.Graph.graph();
	}

	var graphics = Viva.Graph.View.svgGraphics(),

	// ==== GRAPH RENDERING ==== //
	layout = Viva.Graph.Layout.forceDirected(graph, {
		springLength: customSpringLength,
		springCoeff: customSpringCoeff,
		dragCoeff: customDragCoeff,
		gravity: customGravity
	});
	
	var renderer = Viva.Graph.View.renderer(graph, {
		layout: layout,
		graphics : graphics,
		container: document.getElementById('VGcontainer')
	});
	
	for (var i = 0; i < zoomout; i++) {
	   renderer.zoomOut();
	}

	renderer.run();


	graphics.node(function (node) 
	{		
		var radius = node.data.size / 2;

		var newNode = Viva.Graph.svg('g');
		
	   // The function is called every time renderer needs a ui to display node
	   // decide whether to render node as image or circle based on global parameter renderNodesMode. Add more options when decided upon
		if (renderNodesMode === "images")
		{
		var nodeElement = Viva.Graph.svg('image')
			.attr('width', nodeSize)
			.attr('height', nodeSize)
			.link(node.data.url); // node.data holds custom object passed to graph.addNode();	
		}
		else
		{		
		var nodeElement = Viva.Graph.svg('circle').attr('fill', node.data.color).attr('r', radius).attr('stroke', svgStrokeColor).attr('stroke-width', svgStrokeWidth);	
		}
		
		
		newNode.append(nodeElement);
		newNode.attr("cursor", "pointer");

		$(newNode).hover(function ()  // mouse over
		{
			highlightRelatedNodes(node.id, true);
		}, function () { // mouse out
			highlightRelatedNodes(node.id, false)
		}); 

		$(newNode).click(function () {
			if (renderNodesMode==="circles")
			{
				if (lastSelectedNodeId !== "") {
					graphics.getNodeUI(lastSelectedNodeId).children[0].attr("fill", lastSelectedNodeColor);
				}
				lastSelectedNodeId = node.id;
				lastSelectedNodeColor = this.children[0].attr("fill");
				this.children[0].attr("fill", selectedNodeColor);
			}
			else
			{
				//console.log(graphics.getNodeUI(node.id).children[0].href.baseVal);

				if (lastSelectedImgSrc !== "") {
					graphics.getNodeUI(lastSelectedNodeId).children[0].setAttributeNS('http://www.w3.org/1999/xlink', 'href', lastSelectedImgSrc);
				}
				
				lastSelectedNodeId = node.id;
				lastSelectedImgSrc = graphics.getNodeUI(node.id).children[0].href.baseVal;

				if (node.data.machine_type==="personal") {
					this.children[0].setAttributeNS('http://www.w3.org/1999/xlink', 'href', selectedPersonalCompImgUrl);
				}
				else if(node.data.machine_type==="server"){
					this.children[0].setAttributeNS('http://www.w3.org/1999/xlink', 'href', selectedServerImgUrl);
				}
				else{
					console.log("unrecognized machine type: " + node.data.machine_type);
				}
			}
			showMetadata("node", node);	
		});
		
		$(newNode).dblclick(function () {
			layout.pinNode(node, !layout.isNodePinned(node));
			
		});
		newNode
			.attr('fill', node.data.color)
			.attr('id', node.id);

		return newNode;
	}).placeNode(function (nodeUI, pos) 
	{
		var imageOffset = 0;
		if (renderNodesMode==="images")
		{
		imageOffset = 12;	
		}
		nodeUI.attr('transform', 'translate(' + (pos.x - imageOffset) + ',' + (pos.y - imageOffset) + ')');
	});

	var createMarker = function(id) {
			return Viva.Graph.svg('marker')
					.attr('id', id)
					.attr('viewBox', "0 0 10 10")
					.attr('refX', "10")
					.attr('refY', "5")
					.attr('markerUnits', "strokeWidth")
					.attr('markerWidth', "10")
					.attr('markerHeight', "5")
					.attr('orient', "auto");
	},

	marker = createMarker('Triangle');
	marker.append('path').attr('d', 'M 0 0 L 10 5 L 0 10 z').attr('fill', arrowTriangleColor);

	var defs = graphics.getSvgRoot().append('defs');
	if (isDirectedGraph) // isDirectedGraph = global Parameter
	{
	defs.append(marker);
	}

	var geom = Viva.Graph.geom();

	graphics.link(function(link){
		var label = Viva.Graph.svg('text').attr('id','label_'+link.data.id).text(link.data.id);
		graphics.getSvgRoot().childNodes[0].append(label);
		
		var line = Viva.Graph.svg('path')
			.attr('stroke', 'gray')
			.attr('marker-end', 'url(#Triangle)')
			.attr('id', link.data.id);
			
				$(line).click(function () { 
				handleLineClick();
			});		
			$(line).hover(function () { // mouse over
				 handleLineHover();
			}, function () { // mouse out
			   handleLineHoverOut();
			});
			
			var trackedEdge = ""; // the link itself
			
			var handleLineHover = function()
			{
			var actionTime = Number(link.data.action_time) - getTimeOffset();
			actionTime = actionTime+'';
			var actionTimeWithoutMicroSeconds = Number(actionTime.substring(0,13));
			var actionTimeDateFormat = new Date(actionTimeWithoutMicroSeconds);
			var d = actionTimeDateFormat;
			var datestring = d.getDate()  + "-" + (d.getMonth()+1) + "-" + d.getFullYear() + " " + d.getHours() + ":" + d.getMinutes() + ":" + d.getSeconds();
			// console.log("Link from " + link.fromId + " to " + link.toId + " at time: " + datestring);
			line
			.attr('stroke', lineHoverColor)
			.attr('stroke-width', lineHoverStrokeWidth);					
			}

			var handleLineHoverOut = function()
			{
			line
			.attr('stroke', defaultLinkColor)
			.attr('stroke-width', lineWidth);
			}
			
			var handleLineClick = function()
			{	
			console.log("Link from " + link.fromId + " to " + link.toId + " at time: " + link.data.action_time);
			showMetadata("link", link);	
			 $(label).show().delay(3000).fadeOut();;
			}
			
			// set the line color, opacity and width
			line
			.attr('stroke', defaultLinkColor)
			.attr('stroke-width', svgStrokeWidth); 

			return line; 
		}).placeLink(function(linkUI, fromPos, toPos) {
			var toNodeSize = nodeSize,
			fromNodeSize = nodeSize;

			var from = geom.intersectRect(
				fromPos.x - fromNodeSize / 2, // left
				fromPos.y - fromNodeSize / 2, // top
				fromPos.x + fromNodeSize / 2, // right
				fromPos.y + fromNodeSize / 2, // bottom
				fromPos.x, fromPos.y, toPos.x, toPos.y)
			|| fromPos;

			var to = geom.intersectRect(
				toPos.x - toNodeSize / 2, // left
				toPos.y - toNodeSize / 2, // top
				toPos.x + toNodeSize / 2, // right
				toPos.y + toNodeSize / 2, // bottom
				// segment:
				toPos.x, toPos.y, fromPos.x, fromPos.y)
				|| toPos;

			var data = 'M' + from.x + ',' + from.y +
				'L' + to.x + ',' + to.y;

			linkUI.attr("d", data);
			

			
			document.getElementById('label_'+linkUI.attr('id'))
				.attr("display", "none")
				.attr('onclick', '$(this).hide();')
				.attr('cursor', 'pointer')
				.attr('fill', '#00B388')
				.attr("x", (from.x + to.x) / 2)
				.attr("y", (from.y + to.y) / 2);

		}); 

	// Add nodes and links to Graph
	// limit the number of connections displayed. maxNumOfConnectionsDisplayed is in globalParameters.
	var numOfConnectionsToDisplay = Math.min(graphData.edges.length, maxNumOfConnectionsDisplayed);
	for (var i=0; i<numOfConnectionsToDisplay;i++)
	{
		var edge = graphData.edges[i];
		var edgeId = edge._id;
		var source = edge._inV;
		var dest = edge._outV;
		var epoch = edge.epoch;

		if (source === dest) return; // self-loop
		
		addNodeToGraph(source, graphData);
		addNodeToGraph(dest, graphData);
		addLinkToGraph(source, dest, epoch, edgeId);
	}

	setTimeout(function(){ renderer.pause(); }, timeWaitBeforeFreezeAnimation);
}