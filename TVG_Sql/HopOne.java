package com.hpl.hp.com.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HopOne  {
	static Logger log = LoggerFactory.getLogger(HopOne.class);

	private Long test_id, startTime, endTime;
	private Timestamp executionTime;
	private List<Long> vertices, parameters;
	private String query_table,result_table;
	Map<Long, List<List<Long>>> hopMap = new HashMap<>();
	Boolean writeResult = false;
	BasicDataSource datasource;
	private long timeTaken;

	public HopOne(Map<Long, List<List<Long>>> map, Boolean write, BasicDataSource datasourcepara,String querytable,String resulttable) {
		hopMap = map;
		writeResult = write;
		query_table = querytable;
		result_table= resulttable;
		datasource = datasourcepara;
	}

	public HopResult runHopOne() {
		// TODO Auto-generated method stub
		Connection conn = null;
		ResultSet resultset = null;
		Statement stmt = null;
		PreparedStatement pstmt = null;
		Set<Long> neighborset =null;
		log.debug("Hop1 size :"+hopMap.size());	
		
		for(Map.Entry<Long, List<List<Long>>> entry : hopMap.entrySet()) {			
			test_id = entry.getKey();
			vertices = entry.getValue().get(0);
			parameters = entry.getValue().get(1);			
			try{
				conn = datasource.getConnection();// get connection
				conn.setAutoCommit(false);
				stmt = conn.createStatement();
				neighborset= new HashSet<Long>();
				try{
					stmt.executeUpdate("TRUNCATE TABLE tvg4tm.K1");
					conn.commit();
					}catch(Exception ex){
					conn.rollback();
					log.error("Error in Truncating tvg4tm.K1 table inrunHopOne() :"+ex.getMessage());
					ex.printStackTrace();
				}
			StringBuilder sb = new StringBuilder();
			sb.append("(");
			for (Long v : vertices) {
				sb.append(v).append(",");
				}
			sb.setLength(sb.length()-1);
			sb.append(")");
			//System.out.println("Vertex sb :"+sb);
			
			String query = "SELECT DISTINCT source AS neighbor FROM "+query_table 
					+ " WHERE destination in " + sb.toString() + " AND epoch_time >= '" + parameters.get(0)
					+ "' AND epoch_time <='" + parameters.get(1) + "'"
					+ " UNION SELECT DISTINCT destination AS neighbor FROM "+query_table 
					+ " WHERE source in " + sb.toString() + "  AND epoch_time >= '" + parameters.get(0)
					+ "' AND epoch_time <= '" + parameters.get(1) + "'";
System.out.println("Hop1 query :"+query);
			//log.debug("Hop1 query :"+query);
			//startTime = System.currentTimeMillis();
			//executionTime = new Date().getTime();
			
			pstmt = conn.prepareStatement(query);
			resultset = pstmt.executeQuery();
			while (resultset.next()) {
				neighborset.add(resultset.getLong("neighbor"));
			  //  System.out.println("Neighbour:"+resultset.getLong("neighbor"));
			}
			pstmt.close();			
			pstmt = conn.prepareStatement("INSERT INTO tvg4tm.K1 (neighbor) VALUES(?)");

			for (Long i : neighborset) {
				pstmt.setLong(1, i);
				pstmt.addBatch();
			}
			try {
				 pstmt.executeBatch();
				} catch (SQLException e) {
				log.error("Error inserting in tvg4tm.K1 table: pstmt.executeBatch() " + e.getMessage());
				conn.rollback();
			}
			
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
				timeTaken = resultset.getLong("last_statement_duration_us")/1000;
				executionTime = resultset.getTimestamp("statement_start");
				log.debug("Time taken in Hop1 : "+timeTaken);
							
				//System.out.println("Execution time : "+executionTime);
				//System.out.println("current statement :"+resultset.getString("current_statement"));
				
			}

			if(writeResult)
			{
				//Date date = new Date(executionTime);
				Format format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
				pstmt = conn.prepareStatement(
						 " INSERT INTO "+ result_table + "(test_id,time_taken,edges_count,test_idx,platform,execution_time) VALUES('"
						 + test_id + "','" + timeTaken + "','" + neighborset.size() + "','"
						 + parameters.get(2) + "','" + "Vertica" + "','" + format.format(executionTime) + "')");

			try {
				 pstmt.executeUpdate();
				} catch (SQLException e) {
				log.error("Error in inserting in"+result_table +" pstmt.executeBatch() " + e.getMessage());
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
					log.error("Error in closing connections in TVGSimulatorTestcase.runHopOne method"+ ex.getMessage());
				}
			}
		}
		return new HopResult(neighborset.size(), timeTaken);
	}
}
