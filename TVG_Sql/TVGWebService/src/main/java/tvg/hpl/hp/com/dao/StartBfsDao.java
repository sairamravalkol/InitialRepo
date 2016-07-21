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
import java.util.List;
import java.util.Map;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tvg.hpl.hp.com.bean.StartBfsBean;
import tvg.hpl.hp.com.util.DataBaseUtility;

public class StartBfsDao {
	static Logger log = LoggerFactory.getLogger(StartBfsDao.class);
	private BasicDataSource datasource;
	private Connection connection;
	private PreparedStatement pstmt;
	private Statement stmt;
	private ResultSet resultset;
	Map<String, List<List<String>>> subGraph = new HashMap<>();

	public StartBfsDao() {
		// TODO Auto-generated constructor stub
	}

	public StartBfsBean createTaskId(StartBfsBean queryBean) {
		// TODO Auto-generated method stub
		int maxTaskId = 0;
		if (queryBean != null) {
			datasource = DataBaseUtility.getVerticaDataSource();
			try {
				String lock = "lock table tvg4tm.ws_user_query in exclusive mode";
				String query = "INSERT INTO tvg4tm.ws_user_query (task_id,start_time,end_time,vertices,hop,push_when_done,request_status)"
						+ " VALUES(?,?,?,?,?,?,?)";
				String getMaxTaskId = "select max(task_id) from tvg4tm.ws_user_query";
				log.info("Query createTaskId:" + query);
				System.out.println("insert query:" + query);
				connection = datasource.getConnection();
				connection.setAutoCommit(false);
				stmt = connection.createStatement();
				// stmt.execute(lock);

				pstmt = connection.prepareStatement(getMaxTaskId);
				resultset = pstmt.executeQuery();
				while (resultset.next()) {
					maxTaskId = resultset.getInt(1);
					maxTaskId++;
				}
				pstmt.close();
				pstmt = connection.prepareStatement(query);
				log.info("active connection :" + datasource.getNumActive());
				pstmt.setLong(1, maxTaskId);
				pstmt.setLong(2, Long.parseLong(queryBean.getStartTime()));
				pstmt.setLong(3, Long.parseLong(queryBean.getEndTime()));
				pstmt.setString(4, queryBean.getVertices());
				pstmt.setLong(5, Long.parseLong(queryBean.getHop()));
				pstmt.setLong(6, Long.parseLong(queryBean.getPush_when_done()));
				pstmt.setString(7, "recevied");
				int records_update = pstmt.executeUpdate();
				connection.commit();
				queryBean.setTaskId(String.valueOf(maxTaskId));
				queryBean.setStatus("received");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				try {
					if (stmt != null)
						stmt.close();
					if (pstmt != null)
						pstmt.close();
					if (connection != null)
						connection.close();
				} catch (Exception ex) {

				}
			}

		}
		return queryBean;
	}

	public StartBfsBean updateUserRequestStatus(StartBfsBean queryBean) {
		int no_of_rec_update = 0;
		if (queryBean != null) {
			datasource = DataBaseUtility.getVerticaDataSource();
			try {
				// String lock = "lock table tvg4tm.ws_user_query in exclusive
				// mode";
				String query = "Update tvg4tm.ws_user_query set request_status='completed' where task_id="
						+ queryBean.getTaskId();
				connection = datasource.getConnection();
				connection.setAutoCommit(false);
				// stmt.execute(lock);
				pstmt = connection.prepareStatement(query);
				no_of_rec_update = pstmt.executeUpdate();
				connection.commit();
				queryBean.setStatus("completed");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				try {
					if (stmt != null)
						stmt.close();
					if (pstmt != null)
						pstmt.close();
					if (resultset != null)
						resultset.close();
					if (connection != null)
						connection.close();
				} catch (Exception ex) {
				}
			}
		}
		return queryBean;
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				try {
					if (pstmt != null)
						pstmt.close();
					if (resultset != null)
						resultset.close();
					if (connection != null)
						connection.close();
				} catch (Exception ex) {

				}
			}
		}
		return request_status;
	}

	public Map<String, List<List<String>>> getSubgraphWithTaskId(StartBfsBean bfsBean) {

		try {
			datasource = DataBaseUtility.getVerticaDataSource();
			connection = datasource.getConnection();// get connection
			String query = "select source,destination,max(epoch_time) as epoch_time "
					+ " from tvg4tm.ws_result where task_id="
					+ bfsBean.getTaskId() + " group by source,destination limit 1000"; 
			
			/*String query ="SELECT ws.* FROM tvg4tm.ws_result ws INNER JOIN"
					+ " (SELECT source,destination, MAX(epoch_time) AS MaxEpoch"
					+ " FROM tvg4tm.ws_result GROUP BY source,destination) groupedtt "
					+ "ON ws.source = groupedtt.source AND ws.epoch_time = groupedtt.MaxEpoch where task_id="+bfsBean.getTaskId();*/
			log.info("getSubgraphWithTaskId Query:" + query);
			pstmt = connection.prepareStatement(query);
			resultset = pstmt.executeQuery();
			while (resultset.next()) {
				/**
				 * Store Subgraph in a map to create Json format
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
					// System.out.println("mapValue:"+mapValue);
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
			ex.printStackTrace();
		} finally {
			try {
				if (pstmt != null)
					pstmt.close();
				if (resultset != null)
					resultset.close();
				if (connection != null)
					connection.close();
			} catch (Exception ex) {

			}
		}
		return subGraph;
	}
}
