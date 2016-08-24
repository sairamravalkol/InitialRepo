package tvg.hpl.hp.com.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tvg.hpl.hp.com.bean.QueryResultBean;
import tvg.hpl.hp.com.util.ApplicationConstants;
import tvg.hpl.hp.com.util.DataBaseUtility;

public class DeleteGraphDao {
	static Logger log = LoggerFactory.getLogger(DeleteGraphDao.class);
	private BasicDataSource datasource;
	private Connection connection;
	private PreparedStatement pstmt;
	private QueryResultBean QueryResultBean;

	public DeleteGraphDao() {
		// TODO Auto-generated constructor stub
	}

	public QueryResultBean deleteSubGRaphByTaskId(String taskId) {
	
		if (taskId != null) {
			datasource = DataBaseUtility.getVerticaDataSource();
			try {
				String query = "DELETE FROM tvg4tm.ws_result WHERE task_id = " + taskId;
				log.info("DELETE query:" + query);
				connection = datasource.getConnection();
				pstmt = connection.prepareStatement(query);
				int executeUpdate = pstmt.executeUpdate();
				QueryResultBean = new QueryResultBean();
				QueryResultBean.setTaskId(taskId);
				if (executeUpdate==0) {
					QueryResultBean.setStatus("1");

				} else {
					QueryResultBean.setStatus("0");

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
		return QueryResultBean;
	}
	public QueryResultBean updateTokenDeleteStatusByTaskId(String taskId) {
		log.info("Update taskId:"+taskId);
		if (taskId != null && taskId != "") {
			datasource = DataBaseUtility.getVerticaDataSource();
			try {
				String query = "Update tvg4tm.ws_user_query set request_status='"+ApplicationConstants.REQUEST_STATUS_DELETED +"' where task_id=" + taskId;
				System.out.println("Update Query :"+query);
				log.info("Update Query :"+query);
				connection = datasource.getConnection();
				connection.setAutoCommit(false);
				pstmt = connection.prepareStatement(query);
				pstmt.executeUpdate();
				connection.commit();
				QueryResultBean = new QueryResultBean();
				QueryResultBean.setTaskId(taskId);
				QueryResultBean.setStatus(ApplicationConstants.REQUEST_STATUS_DELETED);
			} catch (SQLException e) {
				try {
					connection.rollback();
					log.error("Error in Updating deleted task id Status:"+e.getMessage());
				} catch (SQLException e1) {
					log.error("Error in rollback in updateTokenDeleteStatusByTaskId :"+e1.getMessage());
				}
			} finally {
				try {
					if (pstmt != null)
						pstmt.close();
					if (connection != null)
						connection.close();
				} catch (Exception ex) {
					log.error("Error in closing connections in updateTokenDeleteStatusByTaskId :"+ex.getMessage());
				}
			}
		}
		return QueryResultBean;
	}

}
