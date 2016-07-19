// =================================		UTILS		====================================


// ===========		INITIALIZATION	AND GENERAL   	==============
$(document).ready(function(){
	init();	
});

$(window).resize(function(){
	adjustResolution();
});

function init()
{
console.log("document / web page ready...");
	adjustResolution();
	initializeWidget();
	initWebSocket();
}


function timeComponentVisibility()
{
	if (displayTimeRelatedComponent)
	{
	$(timeFieldsContainer).show();	
	setStartTimeField(predefined_start_time);
	setEndTimeField(predefined_end_time);
	}
	else
	{
	$(timeFieldsContainer).hide();		
	}
}

function headerVisibility()
{
	if (displayHeader)
	{
	$(page_header).show();	
	}
	else
	{
	$(page_header).hide();	
	document.getElementById("main").style.height = "1000px";
	}	
}

function initializeWidget()
{
console.log("initializing widget");
timeComponentVisibility();
headerVisibility();
	if (renderMethod === "webGL")
	{
	listenToContainerClickEvents();
	}
}

function adjustResolution()
{
var screenHeight = $(document).height();
//alert(screenHeight);
var main = document.getElementById("main");
var page_header_height = document.getElementById("page_header").style.height;
var footer_height = document.getElementById("footer").style.height;
var main_height = Math.min(screenHeight- (page_header_height + footer_height + 200), 0.8*screenHeight);
main.style.height = main_height +"px";
}

function listenToContainerClickEvents()
{
document.getElementById("VGcontainer").onclick=function(e)
	{
		if (graph !== null)
		{
		  checkIfMouseOverEdge(e);
		}
	}	
}

// ===== HELPER METHODS USED TO LOCATE WEBGL LINES LINES IN GRAPH ======= //

function checkIfMouseOverEdge(e)
{
var clientPos = {};
var boundRect = renderer.getGraphics().getGraphicsRoot().getBoundingClientRect();
clientPos.x = e.clientX - boundRect.left;
clientPos.y = e.clientY - boundRect.top;
this.renderer.getGraphics().transformClientToGraphCoordinates(clientPos);
resetAllToDefaultEdgeColor();
	graph.forEachLink(function(link) 
	{
		var line =  renderer.getGraphics().getLinkUI(link.id);
		var distanceFromLine = distToSegment(clientPos, line.pos.from, line.pos.to);
		if (distanceFromLine < distanceAccuracyFromLink)
		{
			clearAllEdgeLabels();
			createEdgeLabel(link, e);	
			line.color = -45229240;
			showMetadata("link", link);
		}
	});		
}

function createEdgeLabel(link, e)
{
var label = document.createElement('span');
label.classList.add('edge-label');
label.onclick = function(){console.log('label clicked');}; 
label.id="edgeLabel"+link.id;
label.innerText = link.id;
label.style.left = e.clientX+"px";
label.style.top = e.clientY+"px";
label.style.display="block";
setTimeout(function(){ label.style.display="none"; }, 3000);
document.getElementById('VGcontainer').appendChild(label);	
}

function clearAllEdgeLabels()
{
var edgeLabels = document.getElementsByClassName("edge-label");
var i;
	for (i = 0; i < edgeLabels.length; i++) 
	{
		edgeLabels[i].style.display = "none";
	}
}

function resetAllToDefaultEdgeColor()
{
	graph.forEachLink(function(link) 
	{
		var line =  renderer.getGraphics().getLinkUI(link.id);
		 if (line !== undefined)
		 {
		 line.color = -858993409;
		 }
	});			
}
// Points are represented as objects with x and y attributes.

function sqr(x) { 
    return x * x 
}

function dist2(v, w) { 
    return sqr(v.x - w.x) + sqr(v.y - w.y) 
}

function distToSegmentSquared(p, v, w) {
  var l2 = dist2(v, w);
    
  if (l2 == 0) return dist2(p, v);
    
  var t = ((p.x - v.x) * (w.x - v.x) + (p.y - v.y) * (w.y - v.y)) / l2;
    
  if (t < 0) return dist2(p, v);
  if (t > 1) return dist2(p, w);
    
  return dist2(p, { x: v.x + t * (w.x - v.x), y: v.y + t * (w.y - v.y) });
}

function distToSegment(p, v, w) { 
    return Math.sqrt(distToSegmentSquared(p, v, w));
}

		
// ====== INDICATION AND STATUS RELATED ======= //

function showActiveWSstatus()
{
document.getElementById("ws_indication_circle").style.backgroundColor=ws_active_color;
document.getElementById("ws_indication_circle").title = "connected to " + webSocketURL;
}

function IsJsonString(str) 
{
    try {
        JSON.parse(str);
    } catch (e) {
        return false;
    }
    return true;
}

function showInactiveWSstatus()
{
document.getElementById("ws_indication_circle").style.backgroundColor=ws_inactive_color;
document.getElementById("ws_indication_circle").title = "disconnected from server";
}

// ===== HELPER METHODS USED IN DRAW GRAPH ======= //

function addNodeToGraph(node, graphData)
{
	if (!isExist(node)) 
	{
		
	var nodeSize;
		var vertex_metadata = getVertexInVetricesObject(node, graphData.vertices);
		//console.log(vertex_metadata);
	
		if (vertex_metadata!=null)
		{
			if (vertex_metadata.external_flag=="true")
			{
			nodeColor = external_node_color ;	// external node color is a global variable. if internal, we will use the defaultNodeColor
			}
			else
			{
			nodeColor=defaultNodeColor;	
			}
			
			var vertex_pageRank_value = getVertexInPageRankObject(node, graphData.vertices);
				if (vertex_pageRank_value!=null)
				{
				//nodeSize = pageRank_factor * vertex_pageRank_value; // pageRank_factor = by how much to multiply so that the muliplication would look visually understandable
				var calculatedNodeSize = pageRank_factor * vertex_pageRank_value;
				if (calculatedNodeSize < minNodeSize) { calculatedNodeSize =  minNodeSize;}
				if (calculatedNodeSize > maxNodeSize) { calculatedNodeSize =  maxNodeSize;}
				nodeSize = calculatedNodeSize;
				}
				else
				{
				console.log("vertex_pageRank_value is null");	
				nodeSize=defaultNodeSize;
				}
				
				// TO DO - show image based on machine_type
				// var image_url = 'https://secure.gravatar.com/avatar/91bad8ceeec43ae303790f8fe238164b';
				
		}
		else
		{
			console.log("vertex_metadata is null");
			nodeColor = undefinedNodeColor;
			nodeSize=defaultNodeSize;
		}
		
		var image_url = getImageUrlByType(vertex_metadata.machine_type);
	graph.addNode(node, { size: nodeSize, color: nodeColor, black_list: vertex_metadata.black_list, external_flag: vertex_metadata.external_flag, machine_type: vertex_metadata.machine_type, page_rank: vertex_metadata.page_rank, url: image_url}); // replace hard coded with real data when available
	}
}

function addLinkToGraph(source, dest, epoch, edgeId)
{
	if (graph.getLink(source, dest) == null && graph.getLink(dest, source) == null)
	{
		graph.addLink(source, dest, {
			// add more data as required
			action_time: epoch,
			id: edgeId
		});	
	}		
}

/* ================== Node Data and Metadata object access related functions ================== */

// look for a specific vertex in the object ticket_vetrices_metadata and return the object with that vertex id
function getVertexInVetricesObject(vertex_id, vertices) {
    for (var i = 0; i < vertices.length; i++) {
        if (vertices[i]._id == vertex_id) {
            return vertices[i];
        }

        if (i == vertices.length - 1) {
            console.log("function getVertexInVetricesObject: no vertex with id " + vertex_id + " was found in the metadata located in graphData.vertices object");
        }
    }
}

function getVertexInPageRankObject(vertex_id, vertices){

	if (vertices.length > 0) 
	{	
	   for (var i = 0; i < vertices.length; i++) {
			if (vertices[i]._id == vertex_id) {
				return vertices[i].page_rank;
			}

			if (i == vertices.length - 1) {
				console.log("function getVertexInPageRankObject: no vertex with id " + vertex_id + " was found in the graphData.vertices object ")
			}
		}	
	}
	else
	{
	//	console.log("vertices pagerank array is empty");
	}
}    

function isExist(nodeId) {
// we check the node is not already existing before adding (so we won't count it twice in our legend)
    return graph.getNode(nodeId) == null ? false : true;
}

function getImageUrlByType(type)
{
var url = "https://secure.gravatar.com/avatar/91bad8ceeec43ae303790f8fe238164b";
	if (type!==undefined)
	{
		switch(type) 
		{
			case "personal":
				url = personalCompImgUrl; // global param
				break;
			case "server":
				url = serverImgUrl;  // global param
				break;
			default:
				console.log("Function getImageUrlByType: switch statement hit default code block. Using default image url");
		}	
	}
	else
	{
		console.log("Function getImageUrlByType: Type is undefined. Using default image url");
	}
return url;
}

function getImageClassByType(type)
{
var className = "";
	if (type!==undefined)
	{
		switch(type) 
		{
			case "personal":
				className = "personal"; 
				break;
			case "server":
				className = "server";  
				break;
			default:
				console.log("Function getImageClassByType: switch statement hit default code block. Using default image class");
		}	
	}
	else
	{
		console.log("Function getImageClassByType: Type is undefined. Using default image class");
	}
return className;
}

/* ================== present node/ edge metadata ================== */
function showMetadata(type, obj)
{
 	document.getElementById("metadata_title").innerHTML = type + " " + obj.id + " Metadata";
	var dataAttribute = "data";
	var attributeSpan = "";
	var metadataWrapper = document.getElementById("metadata_fields_wrapper");
	
	for (var name in obj[dataAttribute]) 
	{
	//console.log(name + " : " + obj[dataAttribute][name]);
	var attributeSpan = attributeSpan + "<span class='metadata_label'>"+name+":</span><span class='metadata_field'>"+obj[dataAttribute][name]+"</span>";
	}

	//console.log(attributeSpan);
	metadataWrapper.innerHTML=attributeSpan;
//	alert("now erasing");
//	metadataWrapper.innerHTML="";
}

function updatePlaceholderWithAttribute(label, value)
{
	if (label!==undefined)	
	{
		console.log(label);
	}
	else
	{
		console.log("label is undefined")
	}
	
	if (value!==undefined)	
	{
		console.log(value);
	}
	else
	{
		console.log("value is undefined")
	}
	
}



