package tvg.hpl.hp.com.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tvg.hpl.hp.com.bean.StartBfsBean;
import tvg.hpl.hp.com.util.DataBaseUtility;

public class DeleteGraphDao {
	static Logger log = LoggerFactory.getLogger(DeleteGraphDao.class);
	private BasicDataSource datasource;
	private Connection connection;
	private PreparedStatement pstmt;

	public DeleteGraphDao() {
		// TODO Auto-generated constructor stub
	}

	public StartBfsBean delete(StartBfsBean queryBean) {
		// TODO Auto-generated method stub

		if (queryBean != null) {
			datasource = DataBaseUtility.getVerticaDataSource();
			try {
				String taskId = queryBean.getTaskId();
				String query = "DELETE FROM tvg4tm.ws_result WHERE task_id = " + taskId;
				System.out.println("DELETE query:" + query);
				log.info("Query : " + query);
				connection = datasource.getConnection();
				pstmt = connection.prepareStatement(query);
				int executeUpdate = pstmt.executeUpdate();
				System.out.println("Number of Deleted Records:"+executeUpdate);
				
				queryBean.setTaskId(taskId);
				if (executeUpdate==0) {
					queryBean.setStatus("1");

				} else {
					queryBean.setStatus("0");

				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				try {
					if (pstmt != null)
						pstmt.close();
					if (connection != null)
						connection.close();
				} catch (Exception ex) {

				}
			}

		}
		return queryBean;

	}

}
