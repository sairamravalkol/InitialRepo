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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tvg.hpl.hp.com.bean.StartBfsBean;
import tvg.hpl.hp.com.util.DataBaseUtility;

public class HopOne {
	static Logger log = LoggerFactory.getLogger(HopOne.class);
	private String query_table;
	private String result_table;
	Boolean writeResult = false;
	BasicDataSource datasource;
	StartBfsBean startBfsBean;
	int[] insert_record_count;

	public HopOne(StartBfsBean startbfsbeanpara, String querytable, String resulttable) {
		this.startBfsBean = startbfsbeanpara;
		query_table = querytable;
		result_table = resulttable;
	}

	public int[] findHopOneNeighbours() {
		// TODO Auto-generated method stub
		Connection conn = null;
		ResultSet resultset = null;
		Statement stmt = null;
		PreparedStatement pstmt = null;
		Set<Long> neighbourset = null;
		String vertices = startBfsBean.getVertices();
		String start_time = startBfsBean.getStartTime();
		String end_time = startBfsBean.getEndTime();
		try {
			datasource = DataBaseUtility.getVerticaDataSource();
			conn = datasource.getConnection();// get connection
			conn.setAutoCommit(false);
			stmt = conn.createStatement();
			neighbourset = new HashSet<Long>();
			try {
				stmt.executeUpdate("TRUNCATE TABLE tvg4tm.K1");
				conn.commit();
			} catch (Exception ex) {
				conn.rollback();
				log.error("Error in Truncating tvg4tm.K1 table inrunHopOne() :" + ex.getMessage());
				ex.printStackTrace();
			}
			String query = "SELECT DISTINCT source AS neighbour FROM " + query_table + " WHERE destination in ("
					+ vertices + ") AND epoch_time >= '" + start_time + "' AND epoch_time <='" + end_time + "'"
					+ " UNION SELECT DISTINCT destination AS neighbour FROM " + query_table + " WHERE source in ("
					+ vertices + ")  AND epoch_time >= '" + start_time + "' AND epoch_time <= '" + end_time + "'";
			System.out.println("Hop1 Query :" + query);
			pstmt = conn.prepareStatement(query);
			resultset = pstmt.executeQuery();
			while (resultset.next()) {
				neighbourset.add(resultset.getLong("neighbour"));
				System.out.println("vertica Neighbour:" + resultset.getLong("neighbour"));
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
				log.error("Error inserting in tvg4tm.K1 table: pstmt.executeBatch() " + e.getMessage());
				conn.rollback();
			}
			// Commit the transaction to close the COPY command
			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
				log.error("Error in closing connections in TVGSimulatorTestcase.runHopOne method" + ex.getMessage());
			}
		}
		if (startBfsBean.getHop().equals("2"))
			this.storeHopOneEdges(startBfsBean);
		return insert_record_count;

	}

	public int[] storeHopOneEdges(StartBfsBean startBfsBean) {
		System.out.println("GetMaxEpoch");
		// TODO Auto-generated method stub
		Connection conn = null;
		ResultSet resultset = null;
		Statement stmt = null;
		PreparedStatement pstmt = null;
		PreparedStatement pstmtInsert = null;
		try {
			datasource = DataBaseUtility.getVerticaDataSource();
			conn = datasource.getConnection();// get connection
			conn.setAutoCommit(false);
			stmt = conn.createStatement();
			Long source = 0L;
			Long destination = 0L;
			String vertices = startBfsBean.getVertices();
			String start_time = startBfsBean.getStartTime();
			String end_time = startBfsBean.getEndTime();
			
			String query = "select distinct T.source,T.destination, max(T.epoch_time) as epoch_time  from tvg4tm.dns_data_regression_test T"
					+ "	Join tvg4tm.K1 K1 on T.source=K1.neighbour where destination=" + vertices + "and epoch_time>"
					+ start_time + " and epoch_time<" + end_time
					+ "	group by T.source, T.destination Union select distinct T.source,T.destination,max(T.epoch_time) as epoch_time  "
					+ "	from tvg4tm.dns_data_regression_test T join  tvg4tm.K1 K1 on T.destination =  K1.neighbour "
					+ " where source=" + vertices + " and epoch_time>" + start_time + " and epoch_time<" + end_time
					+ " group by T.source,T.destination;";

			String insert_query = "INSERT INTO tvg4tm.ws_result (task_id,source,destination,epoch_time)"
					+ " VALUES(?,?,?,?)";
			pstmt = conn.prepareStatement(query);
			pstmtInsert = conn.prepareStatement(insert_query);
			resultset = pstmt.executeQuery();
			while (resultset.next()) {
				if (resultset.getLong("source") < resultset.getLong("destination")) {
					destination = resultset.getLong("source");
					source = resultset.getLong("destination");
				} 
				else {
					source = resultset.getLong("source");
					destination = resultset.getLong("destination");
				}
				System.out.println(
						"source:" + source + "dest :" + destination + " epoc time:" + resultset.getLong("epoch_time"));
				
				/**
				 *  Insert subGraph in ws_result table
				 **
				 */
				pstmtInsert.setLong(1, Long.parseLong(startBfsBean.getTaskId()));
				pstmtInsert.setLong(2, source);
				pstmtInsert.setLong(3, destination);
				pstmtInsert.setLong(4, resultset.getLong("epoch_time"));
				pstmtInsert.addBatch();
			
			}
			insert_record_count = pstmtInsert.executeBatch();
			conn.commit();
			System.out.println("number of records inserted:" + Arrays.toString(insert_record_count));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			try {
				conn.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			e.printStackTrace();
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
				log.error("Error in closing connections in TVGSimulatorTestcase.runHopOne method" + ex.getMessage());
			}
		}
		return insert_record_count;
	}
}
