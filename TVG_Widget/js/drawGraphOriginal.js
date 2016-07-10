var drawGraph =function(graphData) 
{
console.log("Function drawGraph: Creating nodes and edges from current batch");
console.log(graphData);
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

graphics = Viva.Graph.View.svgGraphics();

// Add nodes and links to Graph
// limit the number of connections displayed. maxNumOfConnectionsDisplayed is in globalParameters.
var numOfConnectionsToDisplay = Math.min(graphData.edges.length, maxNumOfConnectionsDisplayed);
for (var i=0; i<numOfConnectionsToDisplay;i++)
{
	var edge = graphData.edges[i];
	var source = edge._inV;
	var dest = edge._outV;
	var epoch = edge.epoch;

	if (source === dest) return; // self-loop
	
	addNodeToGraph(source, graphData);
	addNodeToGraph(dest, graphData);
	addLinkToGraph(source, dest, epoch);
}


// ==============  IMPLEMENTING THE GRAPHICS OF THE NODES AND LINKS USING METADATA FROM API  - DRAW IN SVG ================ //
	
graphics.node(function (node) 
{		
	var radius = node.data.size / 2;

	var newNode = Viva.Graph.svg('g');
	
   // The function is called every time renderer needs a ui to display node
   // decide whether to render node as image or circle based on global parameter renderNodesMode. Add more options when decided upon
    if (renderNodesMode === "images")
	{
    var nodeElement = Viva.Graph.svg('image')
        .attr('width', 24)
        .attr('height', 24)
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
		layout.pinNode(node, !layout.isNodePinned(node));
	});
	
	$(newNode).dblclick(function () {
		console.log(node);
		showMetadata("node", node);
	});
	newNode
		.attr('fill', node.data.color)
		.attr('id', node.id);

	return newNode;
}).placeNode(function (nodeUI, pos) 
	{
	nodeUI.attr('transform', 'translate(' + (pos.x) + ',' + (pos.y) + ')');
	})
	.link(function (link) 
	{
		var line = Viva.Graph.svg('line');
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
		console.log("Link from " + link.fromId + " to " + link.toId + " at time: " + datestring);
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
		}
		
		// set the line color, opacity and width
 		line
		.attr('stroke', defaultLinkColor)
		.attr('stroke-width', svgStrokeWidth); 

 		return line; 
	});


// ==== UTILS FOR DRAW GRAPH ==== //
highlightRelatedNodes = function (nodeId, isOn) 
{
	// just enumerate all realted nodes and update link color:
	graph.forEachLinkedNode(nodeId, function (node, link) {
		var linkUI = graphics.getLinkUI(link.id);
		if (linkUI) {
			linkUI.attr('stroke', isOn ? hoverLinkColor : defaultLinkColor);
		}
	});
};

// ==== GRAPH RENDERING ==== //
layout = Viva.Graph.Layout.forceDirected(graph, {
	springLength: customSpringLength,
	springCoeff: customSpringCoeff,
	dragCoeff: customDragCoeff,
	gravity: customGravity
});

	
renderer = Viva.Graph.View.renderer(graph, {
    layout: layout,
	graphics: graphics,
	container: document.getElementById('VGcontainer')
});


for (var i = 0; i < zoomout; i++) {
	   renderer.zoomOut();
}

renderer.run();	
}
/*  ==============   End of drawGraph   ==============   */

