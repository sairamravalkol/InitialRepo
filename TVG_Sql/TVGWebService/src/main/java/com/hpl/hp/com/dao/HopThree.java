package com.hpl.hp.com.dao;

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

import com.hpl.hp.com.bean.StartBfsBean;

import tvg.hpl.hp.com.util.DataBaseUtility;

public class HopThree {
	static Logger log = LoggerFactory.getLogger(HopThree.class);
	
	Map<String, List<List<String>>> subGraph = new HashMap<>();
	List<List<String>> mapValue = new ArrayList<>();
	BasicDataSource datasource;
	Boolean writeResult = false;
	private String query_table, result_table;
	StartBfsBean startBfsBean;
	int[] insert_record_count;

	public HopThree(StartBfsBean startbfsbeanpara, String querytable, String resulttable) {
		this.startBfsBean = startbfsbeanpara;
		query_table = querytable;
		result_table = resulttable;
	}

	public int[]  findHopThreeNeighbours() {
		Connection conn = null;
		ResultSet resultset = null;
		PreparedStatement pstmt = null;
		String vertices = startBfsBean.getVertices();
		String start_time = startBfsBean.getStartTime();
		String end_time = startBfsBean.getEndTime();
		new HopTwo(startBfsBean, query_table, result_table).findHopTwoNeighbours();
		try {
			datasource = DataBaseUtility.getVerticaDataSource();
			conn = datasource.getConnection();
			Set<Long> neighbourset = new HashSet<Long>();
			// Set AutoCommit to false to allow Vertica to reuse the same
			conn.setAutoCommit(false);
			// Drop table and recreate.
			try {
				String truncate = "TRUNCATE TABLE tvg4tm.K3";
				conn.prepareStatement(truncate).execute();
			} catch (Exception ex) {
				log.error("Error in truncating table tvg4tm.k3 :" + ex.getMessage());
				ex.printStackTrace();
			}
			String query = " SELECT DISTINCT destination AS k3_neighbour FROM  " + query_table
					     + " ,TVG4TM.K2 AS k2_neighbours " + " WHERE source = k2_neighbours.k2_neighbours AND epoch_time >="
					     +   start_time + " AND epoch_time <=" + end_time + " AND source not in (" + vertices
					     + " ) UNION SELECT DISTINCT source AS k3_neighbour FROM " + query_table
					     + " ,TVG4TM.K2 AS k2_neighbours " + " WHERE destination = k2_neighbours.k2_neighbours AND epoch_time >="
					     +  start_time + " AND epoch_time <=" + end_time + " AND destination not in (" + vertices + ")"
					     + " UNION SELECT DISTINCT k2_neighbours AS k3_neighbour FROM TVG4TM.K2 WHERE k2_neighbours not in ("
					     + vertices + ")";
			resultset = conn.prepareStatement(query).executeQuery();
			while (resultset.next()) {
				neighbourset.add(resultset.getLong("k3_neighbour"));
			}
			pstmt = conn.prepareStatement("INSERT INTO tvg4tm.K3 (k3_neighbours) VALUES(?)");
			for (Long i : neighbourset) {
				pstmt.setLong(1, i);
				pstmt.addBatch();
			}
			try {
				insert_record_count = pstmt.executeBatch();
				// endTime = System.currentTimeMillis();
			} catch (SQLException e) {
				log.error("Error message in inserting in tvg4tm.K3 table: pstmt.executeBatch() " + e.getMessage());
				conn.rollback();
			}
			log.debug("Edge count in K1 Hop :" + neighbourset.size());
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
				if (resultset != null) {
					resultset.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (Exception ex) {
				log.error("Error in closing connections in TVGSimulatorTestcase.runHopThree()" + ex.getMessage());
			}
		}

		/*if (startBfsBean.getHop().equals("3"))
			this.GetMaxEpoch(start_time, end_time);
		*/
		return insert_record_count;

	}

	public int[] storeHopThreeEdges(StartBfsBean startBfsBean) {
		// TODO Auto-generated method stub
		Connection conn = null;
		ResultSet resultset = null;
		PreparedStatement pstmt = null;
		PreparedStatement pstmtInsert = null;	
		try {
			datasource = DataBaseUtility.getVerticaDataSource();
			conn = datasource.getConnection();// get connection
			conn.setAutoCommit(false);
			Long source = 0L;
			Long destination = 0L;
			Long size_count = 0L;
			String vertices = startBfsBean.getVertices();
			String start_time = startBfsBean.getStartTime();
			String end_time = startBfsBean.getEndTime();

			String query = " select distinct T.source,T.destination, max(T.epoch_time) as epoch_time  from tvg4tm.dns_data_regression_test T  "
						 + " Join tvg4tm.K2 K2 on T.source=K2.K2_neighbours join  tvg4tm.K3 K3 on T.destination = K3.K3_neighbours "
						 + " where epoch_time>" + start_time + " and epoch_time<" + end_time
						 + " group by T.source, T.destination  "
						 + " Union select distinct T.source,T.destination,max(T.epoch_time) as epoch_time  from tvg4tm.dns_data_regression_test T "
						 + " Join tvg4tm.K3 K3 on T.source= K3.K3_neighbours  join  tvg4tm.K2 K2 on T.destination =  K2.K2_neighbours"
						 + " where epoch_time>" + start_time + " and epoch_time<" + end_time
						 + " group by T.source,T.destination";
			// System.out.println("HOP3 Query :"+query);
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
				System.out.println(
						"source:" + source + "dest :" + destination + " epoch time:" + resultset.getLong("epoch_time"));

				/**
				 * Insert subGraph in ws_result table
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
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				if (pstmt != null) {
					pstmt.close();
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
