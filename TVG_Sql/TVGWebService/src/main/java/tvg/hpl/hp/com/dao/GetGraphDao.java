package tvg.hpl.hp.com.dao;

/**
 * @author 
 *
 */
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.dbcp2.BasicDataSource;

import tvg.hpl.hp.com.bean.StartBfsBean;
import tvg.hpl.hp.com.util.DataBaseUtility;

public class GetGraphDao {
	private BasicDataSource datasource;
	private Connection connection;
	private PreparedStatement pstmt;
	private Statement stmt;
	private ResultSet resultset;

	public GetGraphDao() {
		// TODO Auto-generated constructor stub
	}

	public void createTaskId(StartBfsBean queryBean) {
		// TODO Auto-generated method stub

		if(queryBean != null){
			datasource = DataBaseUtility.getVerticaDataSource();
			try {
				String query = "Insert in";
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
		
	}

}
