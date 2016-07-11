package com.hpl.hp.com.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




public class HopTwo  {
	static Logger log = LoggerFactory.getLogger(HopTwo.class);
	private long test_id,startTime,endTime,timeTaken;
	private Timestamp executionTime;
	private int ind;
	private List<Long> vertices,parameters;
	private List<List<Long>> values;
	Map<Long,List<List<Long>>> hopMap=new HashMap<>();
	Boolean writeResult=false;
	BasicDataSource datasource;
	private String query_table,result_table;
	
	public HopTwo(Map<Long,List<List<Long>>> map,Boolean write,BasicDataSource datasourcepara,String querytable,String resulttable) {
		
		hopMap=map;
		writeResult=write;
		datasource = datasourcepara;
		query_table = querytable;
		result_table = resulttable;
	}	
	public HopResult runHopTwo() {
		Connection conn=null;
		ResultSet resultset=null;
		PreparedStatement pstmt=null;
		Statement stmt = null;		
		int[] result =null;
		long hoponexecution_time=0;
		Set<Long> neighborset=null;
		log.debug("Hop2 size :"+hopMap.size());
		for(Map.Entry<Long, List<List<Long>>> entry:hopMap.entrySet()){
			test_id=entry.getKey();
			vertices=entry.getValue().get(0);
			parameters=entry.getValue().get(1);
			//values=entry.getValue();
			//new Hop1(ind, test_id,values);
			Map<Long, List<List<Long>>> innerMap=new HashMap<>();
			innerMap.put(entry.getKey(), entry.getValue()); 

			HopResult myresult = new HopOne(innerMap,false,datasource,query_table,result_table).runHopOne();
			//System.out.println("hoponexecution_time :"+hoponexecution_time);	
			log.debug("Edges got from Hop1:"+myresult.getEdge_count());
			if(myresult.getEdge_count() > 0){
			try {
				conn=datasource.getConnection();
				conn.setAutoCommit(false);
			    neighborset = new HashSet<Long>();
				
				// Drop table and recreate.
				try{
					String truncate="TRUNCATE TABLE tvg4tm.K2";
					//stmt = conn.createStatement();
					//stmt.executeUpdate(truncate);
					//conn.commit();
					conn.prepareStatement(truncate).execute();
					//System.out.println("truncated");
					
				}catch(Exception ex){
				//	conn.rollback();
					ex.printStackTrace();
				}
				StringBuilder sb = new StringBuilder();
				sb.append("(");
				for (Long v : vertices) {
					sb.append(v).append(",");
					}
				sb.setLength(sb.length()-1);
				sb.append(")");
				String query = "SELECT  DISTINCT destination AS k2_neighbors FROM "+query_table +",TVG4TM.K1 AS k1_neighbors " 
						+ " WHERE source = k1_neighbors.neighbor AND epoch_time >="+parameters.get(0)+" AND epoch_time <="+parameters.get(1)+" AND source not in"+sb.toString() 
						+ " UNION SELECT DISTINCT source AS k2_neighbors FROM "+query_table+", TVG4TM.K1 AS k1_neighbors "
						+ " WHERE destination = k1_neighbors.neighbor AND epoch_time >= "+parameters.get(0)+" AND epoch_time <="+parameters.get(1)+" AND destination not in "+sb.toString()
						+ " UNION SELECT DISTINCT neighbor AS k2_neighbors FROM TVG4TM.K1 WHERE neighbor not in "+sb.toString();
				//log.debug("Hop2 :"+query);
				//startTime=System.currentTimeMillis();
				//executionTime=new Date().getTime();
		
				pstmt=conn.prepareStatement(query);
				resultset=pstmt.executeQuery();
				//demo.printStatus();
				while (resultset.next()) {
				//System.out.println("K2 HOP :"+resultset.getLong("k2_neighbors"));
				neighborset.add(resultset.getLong("k2_neighbors"));
				}
				pstmt.close();
				//System.out.println("Size : "+ neighborset.size());
				// Create the prepared statement
				// Set AutoCommit to false to allow Vertica to reuse the same
				
				pstmt = conn.prepareStatement("INSERT INTO tvg4tm.K2 (k2_neighbors) VALUES(?)");
				for (Long i : neighborset) {
					pstmt.setLong(1, i);
					pstmt.addBatch();
				}
				//demo.printStatus();

				try{
					result = pstmt.executeBatch();
					endTime=System.currentTimeMillis();
				} catch (SQLException e) {
					log.error("Error in inserting in tvg4tm.K2 table in HopTwo.runHopTwo() method: pstmt.executeBatch() " + e.getMessage());
					conn.rollback();
				}
				//timeTaken=endTime-startTime+hoponexecution_time;
			//	log.debug("HOP2:TIME2="+timeTaken+"---"+executionTime);
			//	Date date = new Date(executionTime);
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
					//System.out.println("Time taken : "+timeTaken);
					//System.out.println("Execution time : "+executionTime);
					//System.out.println("current statement :"+resultset.getString("current_statement"));
					
				}
				timeTaken=timeTaken+myresult.getTime_taken();
			   	if(writeResult){
			   		Format format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
					pstmt=conn.prepareStatement("INSERT INTO "+result_table+"(test_id,time_taken,edges_count,test_idx,platform,execution_time) VALUES('"+test_id+"','"+timeTaken+"','"+neighborset.size()+"','"+parameters.get(2)+"','"+"Vertica"+"','"+format.format(executionTime)+"')");//,?,?,?,?,?)");
					try{
						pstmt.executeUpdate();
					} catch (SQLException e) {
						log.error("Error in insertion in "+ result_table +"in HopTwo.runHopTwo() method: pstmt.executeBatch() " + e.getMessage());
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
			}
			else{
				log.debug("Edge count in hop one :"+myresult.getEdge_count());
			}
		}
		return new HopResult(neighborset.size(), timeTaken);
	}
}