package tvg.hpl.hp.com.dao;

/**
 * @author 
 *
 */
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.dbcp2.BasicDataSource;

import tvg.hpl.hp.com.bean.StartBfsBean;
import tvg.hpl.hp.com.bean.StatisticsBean;
import tvg.hpl.hp.com.util.DataBaseUtility;

public class GetGraphStatisticsDao {
	private BasicDataSource datasource;
	private Connection connection;
	private PreparedStatement pstmt1;
	private PreparedStatement pstmt2;
	private ResultSet resultset1;
	private ResultSet resultset2;
	private Set<String> vertices;
	private StatisticsBean statBean;
	private int count=0;
	private List<Object> list;

	public GetGraphStatisticsDao() {
		// TODO Auto-generated constructor stub
	}

	public List<Object> fetch(StartBfsBean queryBean) {
		// TODO Auto-generated method stub

		if(queryBean != null){
			datasource = DataBaseUtility.getVerticaDataSource();
			try {
				 vertices = new HashSet<>();
				 list = new ArrayList<>();
				String userquery = "SELECT task_id,start_time,end_time,hop FROM tvg4tm.ws_user_query "
							+ "WHERE task_id="+ queryBean.getTaskId();
				String wsquery = "SELECT source,destination FROM tvg4tm.ws_result "
						+ "WHERE task_id="+ queryBean.getTaskId();
				System.out.println("Query : " + userquery);
				System.out.println("Query : " + wsquery);
				
				connection = datasource.getConnection();
				pstmt1 = connection.prepareStatement(userquery);
				pstmt2 = connection.prepareStatement(wsquery);
				resultset1 = pstmt1.executeQuery();
				resultset2 = pstmt2.executeQuery();
				
				while(resultset1.next()) {
					queryBean.setTaskId(resultset1.getString("task_id"));
					queryBean.setStartTime(resultset1.getString("start_time"));
					queryBean.setEndTime(resultset1.getString("end_time"));
					queryBean.setHop(resultset1.getString("hop"));
				}
				list.add(queryBean);
				
				statBean = new StatisticsBean();
				while(resultset2.next()) {
			        count++;
					vertices.add(resultset2.getString("source"));
					vertices.add(resultset2.getString("destination"));
				}
				vertices.size();
				statBean.setNum_edges(count);
				statBean.setNum_vertices(vertices.size());
				
				list.add(statBean);
				
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally{
				try{
					if(pstmt1 != null)
						pstmt1.close();
					if(pstmt2 != null)
						pstmt2.close();
					if(connection != null)
						connection.close();					
				}catch(Exception ex){
					
				}
			}
			
		}
		return list;
		
	}

}
