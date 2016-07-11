package com.hpl.hp.com.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.dbcp2.BasicDataSource;

import tvg.hpl.hp.com.util.DataBaseUtility;

public class SqlQueryHandler {
	private BasicDataSource datasource;
	private Connection connection;
	private PreparedStatement pstmt;
	private Statement stmt;
	private ResultSet resultset;

	public SqlQueryHandler() {
		// TODO Auto-generated constructor stub
	}	
	public ResultSet selectQuery(String query){
	
		if(query != null){
			datasource = DataBaseUtility.getVerticaDataSource();
			try {
				connection = datasource.getConnection();
				pstmt = connection.prepareStatement(query);
				resultset = pstmt.executeQuery();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally{
				try{
					if(pstmt != null)
						pstmt.close();
					if(connection != null)
						connection.close();					
				}catch(Exception ex){
					
				}
			}
			
		}
		return resultset;		
	}

}
