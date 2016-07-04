function drawGraphWebGl(graphData) 
{
console.log("Function drawGraphWebGl: Creating nodes and edges from current batch");
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


layout = Viva.Graph.Layout.forceDirected(graph, {
	springLength: customSpringLength,
	springCoeff: customSpringCoeff,
	dragCoeff: customDragCoeff,
	gravity: customGravity
});

// ==== UTILS FOR DRAW GRAPH ==== //
highlightRelatedNodes = function (nodeId, isOn) 
{
	// just enumerate all realted nodes and update link color:
	graph.forEachLinkedNode(nodeId, function (node, link) {
		var linkUI = renderer.getGraphics().getLinkUI(link.id);
		if (linkUI) {
			if (isOn)
			{
			linkUI.color = -8582934;
			}
			else
			{
			  graph.forEachLink(function(link) {
				var line =  renderer.getGraphics().getLinkUI(link.id);
				/* line.color = -858993409; */  // yellow: -85899340, pink: -8589934, 
				line.color = -858993409;
				});
			}
		}
	});
};
		
var domLabels = generateDOMLabels(graph);
var nodeColor = defaultNodeColorHex, 
nodeSize = 15;
var graphics = Viva.Graph.View.webglGraphics();
graphics.placeNode(function(ui, pos) {

	
  // This callback is called by the renderer before it updates
  // node coordinate. We can use it to update corresponding DOM
  // label position;

  // we create a copy of layout position
  var domPos = {
	  x: pos.x ,
	  y: pos.y 
  };
  // And ask graphics to transform it to DOM coordinates:
  graphics.transformGraphToClientCoordinates(domPos);

  // then move corresponding dom label to its own position:
  var boundRect = renderer.getGraphics().getGraphicsRoot().getBoundingClientRect();
  // console.log(boundRect);
  //console.log("domPos.y" + domPos.y + "--- boundRect.height + boundRect.top" + Number(boundRect.height));
  var posX=0;
  var posY=0;
  if (domPos.y < 0)
  {
	posY = 0;  
  }
  else if (domPos.y > Number(boundRect.height))
  {
	 posY = Number(boundRect.height);
  }
  else
  {
	posY = domPos.y;  
  }
  
  if (domPos.x < 0)
  {
	posX = 0;  
  }
  else if (domPos.x > Number(boundRect.width - boundRect.left))
  {
	 posX = Number(boundRect.width + boundRect.left - boundRect.left);
  }
  else
  {
	posX = domPos.x;  
  }

  var nodeId = ui.node.id;
  var labelStyle = domLabels[nodeId].style;
/*   labelStyle.left = domPos.x  + 'px';
  labelStyle.top = domPos.y + 'px'; */
  labelStyle.left = posX  + 'px';
  labelStyle.top = posY + 'px'; 

});

				
// tell webgl graphics we want to use custom shader based on the shape method in the global parameters

// render as circles
var circleNode = buildCircleNodeShader();
graphics.setNodeProgram(circleNode);	



var events = Viva.Graph.webglInputEvents(graphics, graph);
events.mouseEnter(function (node) {
//	console.log('Mouse entered node: ' + node.id);
	highlightRelatedNodes(node.id, true);
}).mouseLeave(function (node) {
//	console.log('Mouse left node: ' + node.id);
	highlightRelatedNodes(node.id, false);
}).dblClick(function (node) {
	console.log('Double click on node: ' + node.id);
	console.log(node);
	layout.pinNode(node, !layout.isNodePinned(node));
}).click(function (node) {
	console.log('Single click on node: ' + node.id);
	console.log(node);
	showMetadata("node", node);
});

 
graphics.node(function (node) 
{	
	if (renderNodesMode === "images")
	{
		// we will use a transparent circle and overlapping it will be a label with the image. 
		// all cursor events on top of the label will be propagated to the transparent circle behind it and hence, the behaviour will be preserved.
		// This is done by the css command of pointer-events: none; under the image classes
		var newNode = new WebglCircle(defaultNodeSize, transparentColor);
		return newNode;
	}
	else
	{
		var radius = node.data.size;
		var nodeColor = node.data.color;

		var hexNodeColor = undefinedNodeColorHex;
		if (nodeColor === defaultNodeColor)
		{
			hexNodeColor = defaultNodeColorHex;
		}
		else
		{
			hexNodeColor = external_node_color_hex;
		}

		var newNode = new WebglCircle(radius, hexNodeColor);
		return newNode;
	}	
});  




graphics.link(function(link,pos,ui) {
	return Viva.Graph.View.webglLine("#cccccc");
});

 
renderer = Viva.Graph.View.renderer(graph, {
		layout    : layout,
		graphics  : graphics,
		container : document.getElementById('VGcontainer')
	});
	

						
renderer.run();

	for (var i = 0; i < zoomout; i++) 
	{
		   renderer.zoomOut();
	} 

		
	
	
	function generateDOMLabels(graph) {
	  // this will map node id into DOM element
	  var labels = Object.create(null);
	  graph.forEachNode(function(node) {
		var label = document.createElement('span');
		label.classList.add('node-label');
		var vertex_metadata = getVertexInVetricesObject(node.id, graphData.vertices);	
		var className = getImageClassByType(vertex_metadata.machine_type);
		label.classList.add(className);
		labels[node.id] = label;
		if (renderNodesMode === "images")
		{
		document.getElementById('VGcontainer').appendChild(label);
		}
	  });
	  return labels;
	}
	
}




// Lets start from the easiest part - model object for node ui in webgl
function WebglCircle(size, color) {
	this.size = size;
	this.color = color;
}

// **************************** WebGL Shader ************************* //

// Next comes the hard part - implementation of API for custom shader
// program, used by webgl renderer:
function buildCircleNodeShader() {
	// For each primitive we need 4 attributes: x, y, color and size.
	var ATTRIBUTES_PER_PRIMITIVE = 4,
		nodesFS = [
		'precision mediump float;',
		'varying vec4 color;',
		'void main(void) {',
		'   if ((gl_PointCoord.x - 0.5) * (gl_PointCoord.x - 0.5) + (gl_PointCoord.y - 0.5) * (gl_PointCoord.y - 0.5) < 0.25) {',
		'     gl_FragColor = color;',
		'   } else {',
		'     gl_FragColor = vec4(0);',
		'   }',
		'}'].join('\n'),
		nodesVS = [
		'attribute vec2 a_vertexPos;',
		// Pack clor and size into vector. First elemnt is color, second - size.
		// Since it's floating point we can only use 24 bit to pack colors...
		// thus alpha channel is dropped, and is always assumed to be 1.
		'attribute vec2 a_customAttributes;',
		'uniform vec2 u_screenSize;',
		'uniform mat4 u_transform;',
		'varying vec4 color;',
		'void main(void) {',
		'   gl_Position = u_transform * vec4(a_vertexPos/u_screenSize, 0, 1);',
		'   gl_PointSize = a_customAttributes[1] * u_transform[0][0];',
		'   float c = a_customAttributes[0];',
		'   color.b = mod(c, 256.0); c = floor(c/256.0);',
		'   color.g = mod(c, 256.0); c = floor(c/256.0);',
		'   color.r = mod(c, 256.0); c = floor(c/256.0); color /= 255.0;',
		'   color.a = 1.0;',
		'}'].join('\n');
	var program,
		gl,
		buffer,
		locations,
		utils,
		nodes = new Float32Array(64),
		nodesCount = 0,
		canvasWidth, canvasHeight, transform,
		isCanvasDirty;
	return {
		/**
		 * Called by webgl renderer to load the shader into gl context.
		 */
		load : function (glContext) {
			gl = glContext;
			webglUtils = Viva.Graph.webgl(glContext);
			program = webglUtils.createProgram(nodesVS, nodesFS);
			gl.useProgram(program);
			locations = webglUtils.getLocations(program, ['a_vertexPos', 'a_customAttributes', 'u_screenSize', 'u_transform']);
			gl.enableVertexAttribArray(locations.vertexPos);
			gl.enableVertexAttribArray(locations.customAttributes);
			buffer = gl.createBuffer();
		},
		/**
		 * Called by webgl renderer to update node position in the buffer array
		 *
		 * @param nodeUI - data model for the rendered node (WebGLCircle in this case)
		 * @param pos - {x, y} coordinates of the node.
		 */
		position : function (nodeUI, pos) {
			var idx = nodeUI.id;
			nodes[idx * ATTRIBUTES_PER_PRIMITIVE] = pos.x;
			nodes[idx * ATTRIBUTES_PER_PRIMITIVE + 1] = -pos.y;
			nodes[idx * ATTRIBUTES_PER_PRIMITIVE + 2] = nodeUI.color;
			nodes[idx * ATTRIBUTES_PER_PRIMITIVE + 3] = nodeUI.size;
		},
		/**
		 * Request from webgl renderer to actually draw our stuff into the
		 * gl context. This is the core of our shader.
		 */
		render : function() {
			gl.useProgram(program);
			gl.bindBuffer(gl.ARRAY_BUFFER, buffer);
			gl.bufferData(gl.ARRAY_BUFFER, nodes, gl.DYNAMIC_DRAW);
			if (isCanvasDirty) {
				isCanvasDirty = false;
				gl.uniformMatrix4fv(locations.transform, false, transform);
				gl.uniform2f(locations.screenSize, canvasWidth, canvasHeight);
			}
			gl.vertexAttribPointer(locations.vertexPos, 2, gl.FLOAT, false, ATTRIBUTES_PER_PRIMITIVE * Float32Array.BYTES_PER_ELEMENT, 0);
			gl.vertexAttribPointer(locations.customAttributes, 2, gl.FLOAT, false, ATTRIBUTES_PER_PRIMITIVE * Float32Array.BYTES_PER_ELEMENT, 2 * 4);
			gl.drawArrays(gl.POINTS, 0, nodesCount);
		},
		/**
		 * Called by webgl renderer when user scales/pans the canvas with nodes.
		 */
		updateTransform : function (newTransform) {
			transform = newTransform;
			isCanvasDirty = true;
		},
		/**
		 * Called by webgl renderer when user resizes the canvas with nodes.
		 */
		updateSize : function (newCanvasWidth, newCanvasHeight) {
			canvasWidth = newCanvasWidth;
			canvasHeight = newCanvasHeight;
			isCanvasDirty = true;
		},
		/**
		 * Called by webgl renderer to notify us that the new node was created in the graph
		 */
		createNode : function (node) {
			nodes = webglUtils.extendArray(nodes, nodesCount, ATTRIBUTES_PER_PRIMITIVE);
			nodesCount += 1;
		},
		/**
		 * Called by webgl renderer to notify us that the node was removed from the graph
		 */
		removeNode : function (node) {
			if (nodesCount > 0) { nodesCount -=1; }
			if (node.id < nodesCount && nodesCount > 0) {
				// we do not really delete anything from the buffer.
				// Instead we swap deleted node with the "last" node in the
				// buffer and decrease marker of the "last" node. Gives nice O(1)
				// performance, but make code slightly harder than it could be:
				webglUtils.copyArrayPart(nodes, node.id*ATTRIBUTES_PER_PRIMITIVE, nodesCount*ATTRIBUTES_PER_PRIMITIVE, ATTRIBUTES_PER_PRIMITIVE);
			}
		},
		/**
		 * This method is called by webgl renderer when it changes parts of its
		 * buffers. We don't use it here, but it's needed by API (see the comment
		 * in the removeNode() method)
		 */
		replaceProperties : function(replacedNode, newNode) {},
	};
}

