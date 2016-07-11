package com.hpl.hp.com.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.dbcp2.BasicDataSource;

import com.hpl.hp.com.bean.StartBfsBean;

import tvg.hpl.hp.com.util.DataBaseUtility;

public class StartBfsDao {
	private BasicDataSource datasource;
	private Connection connection;
	private PreparedStatement pstmt;
	private Statement stmt;
	private ResultSet resultset;

	public StartBfsDao() {
		// TODO Auto-generated constructor stub
	}

	public StartBfsBean createTaskId(StartBfsBean queryBean) {
		// TODO Auto-generated method stub
		int maxTaskId = 0;
		
		if(queryBean != null){
			datasource = DataBaseUtility.getVerticaDataSource();
			try {
				String lock = "lock table tvg4tm.ws_user_query in exclusive mode";
				String query = "INSERT INTO tvg4tm.ws_user_query (task_id,start_time,end_time,vertices,hop,push_when_done,status)"
							+ " VALUES(?,?,?,?,?,?,?)";
				String getMaxTaskId = "select max(task_id) from tvg4tm.ws_user_query";
				
				System.out.println("insert query:"+query);
				connection = datasource.getConnection();
				connection.setAutoCommit(false);
				stmt=connection.createStatement();  
			//    stmt.execute(lock); 
			    
			    pstmt = connection.prepareStatement(getMaxTaskId);
			    resultset = pstmt.executeQuery();
			    
			    while(resultset.next()){
			    	maxTaskId = resultset.getInt(1);
			    	maxTaskId++;
			    	System.out.println("maxid :"+maxTaskId);
			    }
				pstmt.close();
				
				pstmt = connection.prepareStatement(query);
				System.out.println("active connection :"+datasource.getNumActive());
				
				pstmt.setLong(1, maxTaskId);
				pstmt.setLong(2, Long.parseLong(queryBean.getStartTime()));
				pstmt.setLong(3, Long.parseLong(queryBean.getEndTime()));
				pstmt.setString(4, queryBean.getVertices());
				pstmt.setLong(5, Long.parseLong(queryBean.getHop()));
				pstmt.setLong(6, Long.parseLong(queryBean.getPush_when_done()));
				pstmt.setString(7, "recevied");
				int records_update = pstmt.executeUpdate();				
				connection.commit();
				System.out.println("Inser records count :"+records_update);
				
				
				queryBean.setTaskId(String.valueOf(maxTaskId));
				queryBean.setStatus("received");
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally{
				try{
					if(stmt !=null)
						stmt.close();
					if(pstmt != null)
						pstmt.close();
					if(connection != null)
						connection.close();					
				}catch(Exception ex){
					
				}
			}
			
		}
		return queryBean;
		
	}

}
