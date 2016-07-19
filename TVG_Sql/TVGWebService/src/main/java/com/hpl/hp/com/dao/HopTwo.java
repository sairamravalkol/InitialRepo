package com.hpl.hp.com.dao;

/**
 * @author sarmaji
 *
 */
import java.io.BufferedWriter;
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

public class HopTwo {
	static Logger log = LoggerFactory.getLogger(HopTwo.class);
	BufferedWriter bufferdWriter;
	Map<String, List<List<String>>> subGraph = new HashMap<>();
	List<List<String>> mapValue = new ArrayList<>();
	Boolean writeResult = false;
	BasicDataSource datasource;
	private String query_table, result_table;
	StartBfsBean startBfsBean;
	int[] insert_record_count;

	public HopTwo(StartBfsBean startbfsbeanpara, String querytable, String resulttable) {
		this.startBfsBean = startbfsbeanpara;
		query_table = querytable;
		result_table = resulttable;
	}

	public int[] findHopTwoNeighbours() {
		Connection conn = null;
		ResultSet resultset = null;
		PreparedStatement pstmt = null;
		Statement stmt = null;
		Set<Long> neighbourset = null;
		String vertices = startBfsBean.getVertices();
		String start_time = startBfsBean.getStartTime();
		String end_time = startBfsBean.getEndTime();

		/**
		 * First get HopOne neighbour.
		 * 
		 */
		new HopOne(startBfsBean, query_table, result_table).findHopOneNeighbours();

		try {
			datasource = DataBaseUtility.getVerticaDataSource();
			conn = datasource.getConnection();
			conn.setAutoCommit(false);
			neighbourset = new HashSet<Long>();
			try {
					String truncate = "TRUNCATE TABLE tvg4tm.K2";
					conn.prepareStatement(truncate).execute();
				} catch (Exception ex) {
				// conn.rollback();
				ex.printStackTrace();
			}
		String query = " SELECT  DISTINCT destination AS k2_neighbours FROM " + query_table
					 + " ,tvg4tm.K1 AS k1_neighbours " + " WHERE source = k1_neighbours.neighbour AND epoch_time >="
					 +   start_time + " AND epoch_time <=" + end_time + " AND source not in(" + vertices
					 + " )  UNION SELECT DISTINCT source AS k2_neighbours FROM " + query_table
					 + " , tvg4tm.K1 AS k1_neighbours " + " WHERE destination = k1_neighbours.neighbour AND epoch_time >= "
					 +   start_time + " AND epoch_time <=" + end_time + " AND destination not in (" + vertices
					 + " ) UNION SELECT DISTINCT neighbour AS k2_neighbours FROM tvg4tm.K1 WHERE neighbour not in ("
					 +  vertices + ")";

		pstmt = conn.prepareStatement(query);
		resultset = pstmt.executeQuery();

		while (resultset.next()) {
			neighbourset.add(resultset.getLong("k2_neighbours"));
		}
		pstmt.close();
		pstmt = conn.prepareStatement("INSERT INTO tvg4tm.K2 (k2_neighbours) VALUES(?)");
		for (Long i : neighbourset) {
			pstmt.setLong(1, i);
			pstmt.addBatch();
		}
		try {
			insert_record_count = pstmt.executeBatch();
		} catch (SQLException e) {
		log.error("Error in inserting in tvg4tm.K2 table in HopTwo.runHopTwo() method: pstmt.executeBatch() "
				+ e.getMessage());
		conn.rollback();
		}
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
				if (resultset != null) {
					resultset.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (Exception ex) {
				log.error("Error in closing connections in HopTwo.runHopTwo() method" + ex.getMessage());
			}

		}

		/*
		 * if (startBfsBean.getHop().equals("2")) this.GetMaxEpoch(start_time,
		 * end_time);
		 */
		return insert_record_count;
	}

	public int[] storeHopTwoEdges(StartBfsBean startBfsBean) {
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
			String vertices = startBfsBean.getVertices();
			String start_time = startBfsBean.getStartTime();
			String end_time = startBfsBean.getEndTime();
			String query = " select distinct T.source,T.destination, max(T.epoch_time) as epoch_time  from tvg4tm.dns_data_regression_test T  "
					+ " Join tvg4tm.K1 K1 on T.source=K1.neighbour join  tvg4tm.K2 K2 on T.destination = K2.K2_neighbours "
					+ " where epoch_time>" + start_time + " and epoch_time<" + end_time
					+ " group by T.source, T.destination  "
					+ " Union select distinct T.source,T.destination,max(T.epoch_time) as epoch_time  from tvg4tm.dns_data_regression_test T "
					+ " Join tvg4tm.K2 K2 on T.source= K2.K2_neighbours  join  tvg4tm.K1 K1 on T.destination =  K1.neighbour"
					+ " where epoch_time>" + start_time + " and epoch_time<" + end_time
					+ "  group by T.source,T.destination limit 10";
			// System.out.println("Query :"+query);
			String insert_query = " INSERT INTO tvg4tm.ws_result (task_id,source,destination,epoch_time)"
								+ " VALUES(?,?,?,?)";
			pstmt = conn.prepareStatement(query);
			pstmtInsert = conn.prepareStatement(insert_query);
			resultset = pstmt.executeQuery();
			while(resultset.next()){
				if (resultset.getLong("source") > resultset.getLong("destination")) {
					destination = resultset.getLong("source");
					source = resultset.getLong("destination");
				} else {
					source = resultset.getLong("source");
					destination = resultset.getLong("destination");
				}
		//	System.out.println("source:" + source + "dest :" + destination + " epoch time:" + resultset.getLong("epoch_time"));

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
			//System.out.println("subgraph map :"+subGraph);
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