package tvg.hpl.hp.com.dao;

/**
 * @author sarmaji
 *
 */

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tvg.hpl.hp.com.bean.QueryResultBean;
import tvg.hpl.hp.com.bean.StartBfsBean;
import tvg.hpl.hp.com.util.ApplicationConstants;
import tvg.hpl.hp.com.util.DataBaseUtility;

public class StartBfsDao {
	static Logger log = LoggerFactory.getLogger(StartBfsDao.class);
	private BasicDataSource datasource;
	private Connection connection;
	private PreparedStatement pstmt;
	private Statement stmt;
	private ResultSet resultset;
	private QueryResultBean queryResultBean;
	Map<String, List<List<String>>> subGraph = new HashMap<>();
	Map<String, List<String>> nodeMetaDataMap = new HashMap<>();
	Set<String> vertices = new HashSet<String>();

	public StartBfsDao() {

	}

	public QueryResultBean createTaskId(StartBfsBean startBfsBean) {
		int maxTaskId = 0;
		if (startBfsBean != null) {
			datasource = DataBaseUtility.getVerticaDataSource();
			try {
				String query = " INSERT INTO tvg4tm.ws_user_query (task_id,start_time,end_time,vertices,hop,push_when_done,request_status)"
						+ " VALUES(?,?,?,?,?,?,?)";
				String getMaxTaskId = "select max(task_id) from tvg4tm.ws_user_query";
				log.info("Create TaskID Query:" + query);
				connection = datasource.getConnection();
				connection.setAutoCommit(false);
				stmt = connection.createStatement();
				pstmt = connection.prepareStatement(getMaxTaskId);
				resultset = pstmt.executeQuery();
				while (resultset.next()) {
					maxTaskId = resultset.getInt(1);
					maxTaskId++;
				}
				pstmt.close();
				pstmt = connection.prepareStatement(query);
				pstmt.setLong(1, maxTaskId);
				pstmt.setLong(2, Long.parseLong(startBfsBean.getStartTime()));
				pstmt.setLong(3, Long.parseLong(startBfsBean.getEndTime()));
				pstmt.setString(4, startBfsBean.getVertices());
				pstmt.setLong(5, Long.parseLong(startBfsBean.getHop()));
				pstmt.setLong(6, Long.parseLong(startBfsBean.getPushWhenDone()));
				pstmt.setString(7, ApplicationConstants.REQUEST_STATUS_RECEIVED);
				pstmt.executeUpdate();
				connection.commit();
				queryResultBean = new QueryResultBean();
				queryResultBean.setTaskId(String.valueOf(maxTaskId));
				queryResultBean.setStatus(ApplicationConstants.REQUEST_STATUS_RECEIVED);
			} catch (SQLException e) {
				try {
					connection.rollback();
					log.error("Error in Creating TaskId:"+e.getMessage());
				} catch (SQLException e1) {
					log.error("Error in rollback :"+e1.getMessage());
				}
			} finally {
				try {
					if (stmt != null)
						stmt.close();
					if (pstmt != null)
						pstmt.close();
					if (connection != null)
						connection.close();
				} catch (Exception ex) {
					log.error(ex.getMessage());
				}
			}
		}
		return queryResultBean;
	}

	public QueryResultBean updateUserRequestStatusByTaskId(String taskId) {
		log.info("Update taskId:"+taskId);
		System.out.println("Update taskId:"+taskId);
		if (taskId != null && taskId != "") {
			datasource = DataBaseUtility.getVerticaDataSource();
			try {
				String query = "Update tvg4tm.ws_user_query set request_status='"+ApplicationConstants.REQUEST_STATUS_COMPLETED +"' where task_id=" + taskId;
				System.out.println("Update Query :"+query);
				log.info("Update Query :"+query);
				connection = datasource.getConnection();
				connection.setAutoCommit(false);
				pstmt = connection.prepareStatement(query);
				pstmt.executeUpdate();
				connection.commit();
				queryResultBean = new QueryResultBean();
				queryResultBean.setTaskId(taskId);
				queryResultBean.setStatus(ApplicationConstants.REQUEST_STATUS_COMPLETED);
			} catch (SQLException e) {
				try {
					connection.rollback();
				} catch (SQLException e1) {
					log.error(e1.getMessage());
				}
				log.error(e.getMessage());
			} finally {
				try {
					if (pstmt != null)
						pstmt.close();
					if (connection != null)
						connection.close();
				} catch (Exception ex) {
					log.error(ex.getMessage());
				}
			}
		}
		return queryResultBean;
	}

	public String checkStatusWithTaskId(String taskId) {
		String request_status = null;
		if (taskId != null && taskId != "") {
			datasource = DataBaseUtility.getVerticaDataSource();
			try {
				String query = "select request_status from tvg4tm.ws_user_query where task_id=" + taskId;
				log.info("checkStatusWithTaskId Query:" + query);
				connection = datasource.getConnection();
				pstmt = connection.prepareStatement(query);
				resultset = pstmt.executeQuery();
				while (resultset.next()) {
					request_status = resultset.getString("request_status");
				}
			} catch (SQLException e) {
				log.error(e.getMessage());
			} finally {
				try {
					if (pstmt != null)
						pstmt.close();
					if (resultset != null)
						resultset.close();
					if (connection != null)
						connection.close();
				} catch (Exception ex) {
					log.error(ex.getMessage());
				}
			}
		}
		return request_status;
	}

	public Map<String, List<List<String>>> getSubgraphWithTaskId(String taskId, String fetchSize) {
		if(taskId !=null && taskId !="" && fetchSize !=null && fetchSize !=""){
		try {
			datasource = DataBaseUtility.getVerticaDataSource();
			connection = datasource.getConnection();
			String query = " select source,destination,max(epoch_time) as epoch_time "
						 + " from tvg4tm.ws_result where task_id=" + taskId + " "
						 + " group by source,destination limit "+fetchSize;
			log.info("Result:getSubgraphWithTaskId Query:" + query);
			pstmt = connection.prepareStatement(query);
			resultset = pstmt.executeQuery();
			while (resultset.next()) {
				/**
				 * Store Subgraph in a map to create JSON format
				 * 
				 */
				String source = String.valueOf(resultset.getLong("source"));
				String destination = String.valueOf(resultset.getLong("destination"));
				String epoch_time = String.valueOf(resultset.getLong("epoch_time"));
				System.out.println("S:" + source + " " + "D:" + destination + " " + "T:" + epoch_time);

				if (subGraph.containsKey(source)) {
					List<List<String>> mapValue = subGraph.get(source);
					List<String> destinationList = mapValue.get(0);
					List<String> epoch_timeList = mapValue.get(1);
					destinationList.add(destination);
					epoch_timeList.add(epoch_time);
					mapValue = new ArrayList<>();
					mapValue.add(destinationList);
					mapValue.add(epoch_timeList);
					subGraph.put(source, mapValue);
				} else {
					List<String> destinationList = new ArrayList<String>();
					List<String> epoch_timeList = new ArrayList<String>();
					List<List<String>> mapValue = new ArrayList<>();
					destinationList.add(destination);
					epoch_timeList.add(epoch_time);
					mapValue.add(destinationList);
					mapValue.add(epoch_timeList);
					subGraph.put(source, mapValue);
				}
			}
		} catch (Exception ex) {
			log.error(ex.getMessage());
		} finally {
			try {
				if (pstmt != null)
					pstmt.close();
				if (resultset != null)
					resultset.close();
				if (connection != null)
					connection.close();
			} catch (Exception ex) {
				log.error(ex.getMessage());

			}
		}
	}
		return subGraph;
}

	public Set<String> getAllVerticesByTaskId(String taskId) {

		try {
			datasource = DataBaseUtility.getVerticaDataSource();
			connection = datasource.getConnection();
			String query = "select source,destination from tvg4tm.ws_result where task_id=" + taskId;
			log.info("getAllVerticesByTaskId Query:" + query);
			pstmt = connection.prepareStatement(query);
			resultset = pstmt.executeQuery();
			while (resultset.next()) {
				String source = String.valueOf(resultset.getLong("source"));
				String destination = String.valueOf(resultset.getLong("destination"));
				vertices.add(source);
				vertices.add(destination);
			}
		} catch (Exception ex) {
			log.error(ex.getMessage());
		} finally {
			try {
				if (pstmt != null)
					pstmt.close();
				if (resultset != null)
					resultset.close();
				if (connection != null)
					connection.close();
			} catch (Exception ex) {
				log.error(ex.getMessage());
			}
		}
		return vertices;
	}

	public Map<String, List<String>> getNodeMetaDataList(String vertices) {
		try {
			datasource = DataBaseUtility.getVerticaDataSource();
			String query = " select node_id,external_flag,machine_type,black_list from tvg4tm.nodes_metadata where"
					+ " node_id in (" + vertices + ")";
			log.info("getNodeMetaDataList Query:" + query);
			System.out.println("getNodeMetaDataList Query:" + query);
			connection = datasource.getConnection();
			pstmt = connection.prepareStatement(query);
			resultset = pstmt.executeQuery();
			while (resultset.next()) {
				List<String> nodeMetaDataList = new ArrayList<String>();
				nodeMetaDataList.add(resultset.getString("external_flag"));
				nodeMetaDataList.add(resultset.getString("machine_type"));
				nodeMetaDataList.add(resultset.getString("black_list"));
				nodeMetaDataMap.put(String.valueOf(resultset.getLong("node_id")), nodeMetaDataList);
			}
		} catch (SQLException e) {
			log.error(e.getMessage());
		} finally {
			try {
				if (pstmt != null)
					pstmt.close();
				if (resultset != null)
					resultset.close();
				if (connection != null)
					connection.close();
			} catch (Exception ex) {
				log.error(ex.getMessage());
			}
		}
		log.info("Node Meta Data Size:"+nodeMetaDataMap.size());
		return nodeMetaDataMap;
	}
}
