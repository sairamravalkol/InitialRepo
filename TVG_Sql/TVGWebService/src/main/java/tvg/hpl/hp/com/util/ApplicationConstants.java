package tvg.hpl.hp.com.util;

/**
 * @author sarmaji
 *
 */
public class ApplicationConstants {
	//TVG Web service properties
	
	public static final String WEB_SOCKET_HOST_NAME = "WEB_SOCKET_HOST";
	public static final String WEB_SOCKET_PORT = "PORT";
	
	// DB server Properties
	public static final String DB_DRIVER = "VERTICA_DB_DRIVER_CLASS";
	public static final String DB_URL = "VERTICA_DB_URL";
	public static final String DB_USERNAME = "VERTICA_DB_USERNAME";
	public static final String DB_PASSWORD = "VERTICA_DB_PASSWORD";
	
	// DB Table Properties	
	public static final String DB_SCHEMA =  "SCHEMA";
	public static final String TEST_TABLE = "TEST_TABLE";
	public static final String QUERY_TABLE = "QUERY_TABLE";
	public static final String RESULT_TABLE= "RESULT_TABLE";
	public static final String RECORDS_FETCH_SIZE= "FETCH_SIZE";
	
	// Web Service Properties
	public static final String PUSH_WHEN_DONE_TRUE = "1";
	public static final String PUSH_WHEN_DONE_FALSE= "0";
	
	//StartBFS API Parameter
	public static final String STARTBFS_START_TIME = "startTime";
	public static final String STARTBFS_END_TIME = "endTime";
	public static final String STARTBFS_HOP = "hop";
	public static final String STARTBFS_VERTICES = "vertices";
	public static final String STARTBFS_PUSH_WHEN_DONE = "push_when_done";
	
	// Request Status Properties
	public static final String REQUEST_STATUS_RECEIVED= "received";
	public static final String REQUEST_STATUS_PROCESSING= "computing";
	public static final String REQUEST_STATUS_COMPLETED= "completed";
	public static final String REQUEST_STATUS_DELETED= "deleted";
	
	// Response Status Properties	
	public static final String RESPONSE_STATUS_SUCCESSFUL= "0";
	public static final String RESPONSE_STATUS_FAIL= "1";
	public static final String RESPONSE_STATUS_COMPUTING= "2";
	public static final String RESPONSE_STATUS_DELETED= "3";
	
	// Hop Properties
	public static final String TVG_HOP_ONE= "1";
	public static final String TVG_HOP_TWO= "2";
	public static final String TVG_HOP_THREE= "3";
	
	// Graph Properties
	public static final String TASKID= "taskId";
	
	//Graphson Json Properties
	public static final String GRAPHSON_JSON_PATH="GRAPHSON_JSON_DIR_PATH";
	
	//Response Status Code
	public static final int RESPONSE_CODE_OK = 200;
	public static final int RESPONSE_CODE_ACCEPTED = 202;
	public static final int RESPONSE_CODE_BAD_REQUEST = 400;
	public static final int RESPONSE_CODE_INTERNAL_SERVER_ERROR = 500;
	
	
	
	
	
	
	
	


}
