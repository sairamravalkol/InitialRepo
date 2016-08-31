// ==========================		GLOBAL PARAMETERS		=========================


/* ====== General ====== */
var ticket_vetrices_metadata; // keeps track of the vertices metadata
var vertices_pagerank;  // keeps track of the vertices_pagerank
var displayHeader = true;

/* ====== Colors ====== */
var defaultLinkColor = "gray";
var defaultLinkColorHex = "0xcccccc";
var hoverLinkColor = "red";
var hoverLinkColorHex = "0xcccccc";
var external_node_color = "#FFD042"; /* orange */
var external_node_color_hex = "0xFFD042"; /* orange for use with webGL*/
var newLinkColorInPlayMode = "#fa6400";
var defaultNodeColor = "#7AD3EA";  /* light blue */
var defaultNodeColorHex = "0x7AD3EA"; /* light blue for use with webGL*/
var lineHoverColor  = "purple";
var lineClickColor  = "red";
var ws_inactive_color = "red";
var ws_active_color = "#69c773";
var svgStrokeColor = "black";
var undefinedNodeColor = "pink";
var undefinedNodeColorHex = "0xFFC0CB";
var arrowTriangleColor = "white";
var transparentColor = "0x00374753";
var selectedNodeColorWebGl = 0xFFA500FF;
var selectedLineColorWebGL = -95529240;

/* ====== Sizes ====== */
var defaultNodeSize = 15;
var enlargedNodeSize = 35;
var pageRank_factor = 25;
var minNodeSize = 15;
var maxNodeSize = 35;
var lineHoverStrokeWidth = 2;
var lineClickStrokeWidth = 2;
var lineWidth = 1;
var svgStrokeWidth = 1;
var maxNumOfConnectionsDisplayed = 1000;
var nodeSize = 24; // size of node size for svg images

/* ====== Graph Related ====== */
var renderMethod = "webGL";   // options: webGL ,  SVG
var renderNodesMode = "circles"; // options: circles , images
var zoomout = 7;
var isDirectedGraph = false;
var graph = null;
var lastLink = null;
var graphics = null;
var layout = null;
var renderer = null;
var firstTimeGraphDraw = true;
var distanceAccuracyFromLink = 1; // the distance allowed when clicking a link
var timeWaitBeforeFreezeAnimation = 8000; // after this many milliseconds the graph freezes and stops rendering.
var selectedNodeColor = "red";
var selectedBorderColor = "red";
var lastSelectedNodeId="";
var lastSelectedNodeColor="";

// Graph initialization params
var	customSpringLength =  300;
var	customSpringCoeff = 0.0005;
var	customDragCoeff = 0.05;
var	customGravity = -1.2;

// Graph Images
var personalCompImgUrl = "img/icons/personal.gif" ;
var serverImgUrl = "img/icons/server.gif";
var selectedPersonalCompImgUrl = "img/icons/selectedPersonal.gif" ;
var selectedServerImgUrl = "img/icons/selectedServer.gif" ;
var lastSelectedImgSrc = "";

/* ====== Time Related ====== */
var displayTimeRelatedComponent = false;
var predefined_start_time="1434567600000000";  // should come from ws or rest api
var predefined_end_time="1434585599997777";	   // should come from ws or rest api

/* ====== Server Interaction ====== */

// == REST == //
var anonymiseServerUrlOld = "http://db4s-de-anonymize-service.hpl.hp.com:3001/";
var anonymiseServerUrl = "http://db4s-de-anonymize-service.labs.hpecorp.net:3001/";

var webServerAddressOld = "http://isaac.hpl.hp.com";
var webServerAddress = "http://isaac.labs.hpecorp.net";
var directory = "api/"; // this is the directory where the api's are. This was a CR by UI company. 
// var directory = ""; // this is the directory where the api's are. This was a CR by UI company.

// == Web Socket == //
//var webSocketURLOld = "ws://l4tm-tvg-ha01.hpl.hp.com:8083";
var webSocketURL = "ws://l4tm-tvg-ha01.labs.hpecorp.net:8083";