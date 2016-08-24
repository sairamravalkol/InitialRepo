package tvg.hpl.hp.com.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.dbcp2.BasicDataSource;

import tvg.hpl.hp.com.bean.QueryResultBean;
import tvg.hpl.hp.com.bean.StartBfsBean;
import tvg.hpl.hp.com.util.DataBaseUtility;

/**
 * "ShowGraphDao" class represents to use vertica database to check StatusID  get corresponding
 *  taskID from vertica and store it into StartBfsBean Instance.
 *  
 * @author SAIRAM RAVALKOL
 * @author JIBON JYOTHI SHARMA
 * @version 1.0 25/07/2016
 */

public class ShowGraphDao {
	static Logger log = LoggerFactory.getLogger(ShowGraphDao.class);

	private BasicDataSource datasource;
	private Connection connection;
	private PreparedStatement pstmt;
	private ResultSet resultset;
	private QueryResultBean responseBean;

	public ShowGraphDao() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * Returns the StartBfsBean object having status and TaskId.
	 * 
	 * @param queryBean StartBfsBean object having taskId
	 * @return instance of StartBfsBean.
	 */
	public QueryResultBean checkTaskIdStatus(String taskId) {

		// checking queryBean object is null or not
		if(taskId != null) {
			datasource = DataBaseUtility.getVerticaDataSource();
			try {
				String query ="SELECT task_id,request_status FROM tvg4tm.ws_user_query WHERE task_id = " + taskId;
				System.out.println("Query : " + query);
				log.info("checkTaskIdStatus:"+query);
				connection = datasource.getConnection();
				connection.setAutoCommit(false);
				pstmt = connection.prepareStatement(query);
				resultset = pstmt.executeQuery();
				// setting  task id and status code into queryBean instance
				while(resultset.next())
				{
					responseBean = new QueryResultBean();
					responseBean.setTaskId(resultset.getString("task_id"));
					responseBean.setStatus(resultset.getString("request_status"));
				} // end while
			} catch (SQLException e) {
				log.error("Error in checkTaskIdStatus method:"+e.getMessage());
			}finally{
				try{
					if(pstmt != null)
						pstmt.close();
					if(connection != null)
						connection.close();					
				}catch(Exception ex){
					log.error(ex.getMessage());
				} // inner catch
			} // end finally block

		} // end if

		return responseBean;
	} // end checkTaskIdStatus()

} // end ShowGraphDao
