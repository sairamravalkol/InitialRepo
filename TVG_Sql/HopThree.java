package com.hpl.hp.com.dao;

import java.sql.Connection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HopThree {
	static Logger log = LoggerFactory.getLogger(HopThree.class);
	private long test_id, startTime, endTime,timeTaken;
	private Timestamp executionTime;
	private int ind;
	private List<Long> vertices, parameters;
	private List<List<Long>> values;

	Map<Long, List<List<Long>>> hopMap = new HashMap<>();
	BasicDataSource datasource;
	Boolean writeResult = false;
	private String query_table,result_table;

	public HopThree( Map<Long, List<List<Long>>> map, Boolean write, BasicDataSource datasourcepara,String querytable,String resulttable) {
		
		hopMap = map;
		writeResult = write;
		datasource = datasourcepara;
		query_table = querytable;
		result_table = resulttable;
	}
	public void runHopThree() {
		Connection conn = null;
		ResultSet resultset = null;
		PreparedStatement pstmt = null;		
		log.debug("HOP3 Size :"+hopMap.size());
		
		for (Map.Entry<Long, List<List<Long>>> entry : hopMap.entrySet()) {		
		// demo.printStatus();
		long hopone_hoptwo_execution_time=0;
		test_id = entry.getKey();
		vertices = entry.getValue().get(0);
		parameters = entry.getValue().get(1);		
		Map<Long, List<List<Long>>> innerMap=new HashMap<>();
		innerMap.put(entry.getKey(), entry.getValue()); 
		
		HopResult myresult= new HopTwo(innerMap, false, datasource,query_table,result_table).runHopTwo();		
		log.debug("Edges got from Hop2 :"+myresult.getEdge_count());
		if(myresult.getEdge_count() > 0){
		try {
			conn = datasource.getConnection();
			Set<Long> neighborset = new HashSet<Long>();
			// Set AutoCommit to false to allow Vertica to reuse the same
			conn.setAutoCommit(false);
			// Drop table and recreate.
			try {
				String truncate = "TRUNCATE TABLE tvg4tm.K3";
				conn.prepareStatement(truncate).execute();
			} catch (Exception ex) {
				log.error("Error in truncating table tvg4tm.k3 :"+ex.getMessage());
				ex.printStackTrace();
			}
			StringBuilder sb = new StringBuilder();
			sb.append("(");
			for (Long v : vertices) {
				sb.append(v).append(",");
				}
			sb.setLength(sb.length()-1);
			sb.append(")");
			String query = "SELECT DISTINCT destination AS k3_neighbor FROM  "+query_table+",TVG4TM.K2 AS k2_neighbors "
					+ " WHERE source = k2_neighbors.k2_neighbors AND epoch_time >=" + parameters.get(0)
					+ " AND epoch_time <=" + parameters.get(1) + " AND source not in" + sb.toString()
					+ " UNION SELECT DISTINCT source AS k3_neighbor FROM "+query_table+",TVG4TM.K2 AS k2_neighbors "
					+ " WHERE destination = k2_neighbors.k2_neighbors AND epoch_time >=" + parameters.get(0)
					+ " AND epoch_time <=" + parameters.get(1) + " AND destination not in " + sb.toString()
					+ " UNION SELECT DISTINCT k2_neighbors AS k3_neighbor FROM TVG4TM.K2 WHERE k2_neighbors not in "
					+ sb.toString();
			//log.debug("Hop3 query :"+query);
			//startTime = System.currentTimeMillis();
			//executionTime = new Date().getTime();
			resultset = conn.prepareStatement(query).executeQuery();
			// demo.printStatus();
			while (resultset.next()) {
				neighborset.add(resultset.getLong("k3_neighbor"));
			}
			pstmt = conn.prepareStatement("INSERT INTO tvg4tm.K3 (k3_neighbor) VALUES(?)");
			for (Long i : neighborset) {
				pstmt.setLong(1, i);
				pstmt.addBatch();
			}

			try {
				pstmt.executeBatch();
				//endTime = System.currentTimeMillis();
			} catch (SQLException e) {
				log.error("Error message in inserting in tvg4tm.K3 table: pstmt.executeBatch() " + e.getMessage());
				conn.rollback();
			}
			//long timeTaken = endTime - startTime+hopone_hoptwo_execution_time;
			//System.out.println("HOP3:TIME3=" + timeTaken + "---" + executionTime);
			//Date date = new Date(executionTime);
			log.debug("Edge count in K1 Hop :"+neighborset.size());
			pstmt.close();
			
			resultset.close();
			//endTime = System.currentTimeMillis();
			//timeTaken = endTime - startTime;
			//System.out.println("JDBC execution TIME=" + timeTaken);
			pstmt= conn.prepareStatement("select * from current_session");
			resultset= pstmt.executeQuery();
			while(resultset.next()){
				//System.out.println("Last stmt:"+resultset.getString("last_statement"));
				//System.out.println("last stmt duration :"+resultset.getLong("last_statement_duration_us"));
				//System.out.println("statement start :"+resultset.getTimestamp("statement_start"));
				
				timeTaken = resultset.getLong("last_statement_duration_us")/1000; //converting microseconds to milliseconds
				executionTime = resultset.getTimestamp("statement_start");
				//System.out.println("Time taken : "+timeTaken);
				//System.out.println("Execution time : "+executionTime);
				//System.out.println("current statement :"+resultset.getString("current_statement"));
				
			}
			timeTaken=timeTaken+myresult.getTime_taken();
			if (writeResult) {
				Format format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
				pstmt = conn.prepareStatement(
						"INSERT INTO "+result_table+"(test_id,time_taken,edges_count,test_idx,platform,execution_time) VALUES('"
						+ test_id + "','" + timeTaken + "','" + neighborset.size() + "','"
						+ parameters.get(2) + "','" + "Vertica" + "','" + format.format(executionTime) + "')");
				try {
					pstmt.executeUpdate();
				} catch (SQLException e) {
					log.error("Error in insertion in "+result_table +" in HopThree.runHop3() method : pstmt.executeBatch() " + e.getMessage());
					conn.rollback();
				}
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
				if (resultset != null) {
					resultset.close();
				}
				if (conn != null) {
					conn.close();
				}
				/*if(datasource !=null){
					datasource.close();
				}*/
			} catch (Exception ex) {
				log.error("Error in closing connections in TVGSimulatorTestcase.runHopThree()"
						+ ex.getMessage());
				}
			}
		}
		else{
			log.debug("Edge count in hop two :"+myresult.getEdge_count());
		}
		}
	}
}