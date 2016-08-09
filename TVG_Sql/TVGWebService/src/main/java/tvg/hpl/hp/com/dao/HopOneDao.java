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
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tvg.hpl.hp.com.bean.StartBfsBean;
import tvg.hpl.hp.com.util.ApplicationConstants;
import tvg.hpl.hp.com.util.DataBaseUtility;

public class HopOneDao {
	static Logger log = LoggerFactory.getLogger(HopOneDao.class);
	private String query_table;
	private BasicDataSource datasource;
	private StartBfsBean startBfsBean;
	private String taskId;
	int[] insert_record_count;

	public HopOneDao(StartBfsBean startBfsBeanPara, String taskIdPara, String querytable, String resulttable) {
		this.startBfsBean = startBfsBeanPara;
		this.taskId = taskIdPara;
		this.query_table = querytable;
	}

	public int findHopOneNeighbours() {
		
		Connection conn = null;
		ResultSet resultset = null;
		Statement stmt = null;
		PreparedStatement pstmt = null;
		Set<Long> neighbourset = null;
		
		String vertices = startBfsBean.getVertices();
		String start_time = startBfsBean.getStartTime();
		String end_time = startBfsBean.getEndTime();
		
		log.info("Find Hop One Neighbours and Store in Table for vertex:"+vertices);
		System.out.println("Find Hop One Neighbours");
		
		if (vertices != null && vertices != "" && start_time != null && start_time != "" && end_time != null
				&& end_time != "") {			
			
			try {
				datasource = DataBaseUtility.getVerticaDataSource();
				conn = datasource.getConnection();
				conn.setAutoCommit(false);
				stmt = conn.createStatement();
				neighbourset = new HashSet<Long>();
				try {
					stmt.executeUpdate("TRUNCATE TABLE tvg4tm.K1");
					conn.commit();
				} catch (Exception ex) {
					conn.rollback();
					log.error("Error in Truncating tvg4tm.K1 table in findHopOneNeighbours :" + ex.getMessage());
				}
				String query = " SELECT DISTINCT source AS neighbour FROM " + query_table + " WHERE destination in ("
						+ vertices + ") AND epoch_time >= '" + start_time + "' AND epoch_time <='" + end_time + "'"
						+ " UNION SELECT DISTINCT destination AS neighbour FROM " + query_table + " WHERE source in ("
						+ vertices + ")  AND epoch_time >= '" + start_time + "' AND epoch_time <= '" + end_time + "'";
				log.info("Find Hop One Neighbours Query:" + query);
				pstmt = conn.prepareStatement(query);
				resultset = pstmt.executeQuery();
				while (resultset.next()) {
					neighbourset.add(resultset.getLong("neighbour"));
					log.info("HOP One Neighbours:" + resultset.getLong("neighbour"));
				}
				pstmt.close();
				pstmt = conn.prepareStatement("INSERT INTO tvg4tm.K1 (neighbour) VALUES(?)");
				for (Long i : neighbourset) {
					pstmt.setLong(1, i);
					pstmt.addBatch();
				}
				try {
					insert_record_count = pstmt.executeBatch();
				} catch (SQLException e) {
					log.error("Error inserting in tvg4tm.K1 table: pstmt.executeBatch() :" + e.getMessage());
					conn.rollback();
				}
				// Commit the transaction to close the COPY command
				conn.commit();
				log.info("Size of HopOne Neighbours :"+neighbourset.size());
			} catch (SQLException e) {
				log.error(e.getMessage());
				try {
					conn.rollback();
				} catch (SQLException e1) {
					log.error("Error in rollback in findHopOneNeighbours:"+e1.getMessage());
				}
			} finally {
				try {
					if (pstmt != null) {
						pstmt.close();
					}
					if (stmt != null) {
						stmt.close();
					}
					if (conn != null) {
						conn.close();
					}
					if (resultset != null) {
						resultset.close();
					}
				} catch (Exception ex) {
					log.error("Error in closing connections in HopOne.findHopOneNeighbours() method" + ex.getMessage());
				}
			}
			if (startBfsBean.getHop().equalsIgnoreCase(ApplicationConstants.TVG_HOP_TWO) || startBfsBean.getHop().equalsIgnoreCase(ApplicationConstants.TVG_HOP_THREE))
				this.storeHopOneEdges(startBfsBean, taskId);
		}
		else{
			log.error("Null Parameters are passed to the HopOne.findHopOneNeighbours method");
		}
		return insert_record_count.length;
	}

	public int storeHopOneEdges(StartBfsBean startBfsBean, String taskId) {
		log.info("Find Hope one Edges and Store in Result Table");
		System.out.println("Find Hope one Edges and Store in Result Table");
		String vertices = startBfsBean.getVertices();
		String start_time = startBfsBean.getStartTime();
		String end_time = startBfsBean.getEndTime();
		if (vertices != null && vertices != "" && start_time != null && start_time != "" && end_time != null
				&& end_time != "") {		
		Connection conn = null;
		ResultSet resultset = null;
		Statement stmt = null;
		PreparedStatement pstmt = null;
		PreparedStatement pstmtInsert = null;
		try {
			datasource = DataBaseUtility.getVerticaDataSource();
			conn = datasource.getConnection();
			conn.setAutoCommit(false);
			stmt = conn.createStatement();
			Long source = 0L;
			Long destination = 0L;
			
			String query = " select distinct T.source,T.destination, max(T.epoch_time) as epoch_time  from tvg4tm.dns_data_regression_test T"
					+ " Join tvg4tm.K1 K1 on T.source=K1.neighbour where destination=" + vertices + "and epoch_time>"
					+   start_time + " and epoch_time<" + end_time
					+ " group by T.source, T.destination Union select distinct T.source,T.destination,max(T.epoch_time) as epoch_time  "
					+ " from tvg4tm.dns_data_regression_test T join  tvg4tm.K1 K1 on T.destination =  K1.neighbour "
					+ " where source=" + vertices + " and epoch_time>" + start_time + " and epoch_time<" + end_time
					+ " group by T.source,T.destination;";

			String insert_query = " INSERT INTO tvg4tm.ws_result (task_id,source,destination,epoch_time)"
					+ " VALUES(?,?,?,?)";
			pstmt = conn.prepareStatement(query);
			pstmtInsert = conn.prepareStatement(insert_query);
			resultset = pstmt.executeQuery();
			while (resultset.next()) {
				if (resultset.getLong("source") < resultset.getLong("destination")) {
					destination = resultset.getLong("source");
					source = resultset.getLong("destination");
				} else {
					source = resultset.getLong("source");
					destination = resultset.getLong("destination");
				}
			//	log.info("source:" + source + "dest :" + destination + " epoc time:" + resultset.getLong("epoch_time"));

				/**
				 * Insert subGraph in ws_result table
				 **
				 */
				pstmtInsert.setLong(1, Long.parseLong(taskId));
				pstmtInsert.setLong(2, source);
				pstmtInsert.setLong(3, destination);
				pstmtInsert.setLong(4, resultset.getLong("epoch_time"));
				pstmtInsert.addBatch();
			}
			insert_record_count = pstmtInsert.executeBatch();
			conn.commit();
			log.info("Result Table : Number of records inserted:" + insert_record_count.length);
		} catch (Exception e) {
			try {
				conn.rollback();
				log.error("Error executing the Sql queries in storeHopOneEdges, rollback:"+e.getMessage());
			} catch (SQLException e1) {
				log.error("Error in roll back in storeHopOneEdges:"+e1.getMessage());
			}
		} finally {
			try {
				if (pstmt != null) {
					pstmt.close();
				}
				if (stmt != null) {
					stmt.close();
				}
				if (conn != null) {
					conn.close();
				}
				if (resultset != null) {
					resultset.close();
				}
			} catch (Exception ex) {
				log.error("Error in closing connections in storeHopOneEdges method" + ex.getMessage());
			}
		}
		}
		return insert_record_count.length;
	}
}
