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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tvg.hpl.hp.com.bean.StartBfsBean;
import tvg.hpl.hp.com.bean.GraphStatisticsResultBean;
import tvg.hpl.hp.com.util.DataBaseUtility;

public class GetGraphStatisticsDao {
	static Logger log = LoggerFactory.getLogger(GetGraphStatisticsDao.class);
	private BasicDataSource datasource;
	private Connection connection;
	private PreparedStatement pstmt1;
	private PreparedStatement pstmt2;
	private ResultSet resultset1;
	private ResultSet resultset2;
	private Set<String> vertices;
	private GraphStatisticsResultBean graphStatisticsResultBean;
	private int count = 0;
	private List<Object> list;
	private StartBfsBean startBfsBean;

	public GetGraphStatisticsDao() {

	}

	public List<Object> getGraphstatistics(String taskId) {
		if (taskId != null && taskId != "") {
			datasource = DataBaseUtility.getVerticaDataSource();
			try {
				vertices = new HashSet<>();
				list = new ArrayList<>();
				String userquery = "SELECT task_id,start_time,end_time,hop FROM tvg4tm.ws_user_query "
						+ "WHERE task_id=" + taskId;
				String wsquery = "SELECT source,destination FROM tvg4tm.ws_result " + "WHERE task_id=" + taskId;
				System.out.println("User Query : " + userquery);
				System.out.println("Result Query : " + wsquery);

				connection = datasource.getConnection();
				pstmt1 = connection.prepareStatement(userquery);
				pstmt2 = connection.prepareStatement(wsquery);
				resultset1 = pstmt1.executeQuery();
				resultset2 = pstmt2.executeQuery();
				startBfsBean = new StartBfsBean();
				while (resultset1.next()) {
					startBfsBean.setStartTime(resultset1.getString("start_time"));
					startBfsBean.setEndTime(resultset1.getString("end_time"));
					startBfsBean.setHop(resultset1.getString("hop"));
				}
				list.add(startBfsBean);

				graphStatisticsResultBean = new GraphStatisticsResultBean();
				while (resultset2.next()) {
					count++;
					vertices.add(resultset2.getString("source"));
					vertices.add(resultset2.getString("destination"));
				}
				vertices.size();
				graphStatisticsResultBean.setNum_edges(count);
				graphStatisticsResultBean.setNum_vertices(vertices.size());
				list.add(graphStatisticsResultBean);
			} catch (SQLException e) {
				log.error(" error in getGraphstatistics:" + e.getMessage());
			} finally {
				try {
					if (pstmt1 != null)
						pstmt1.close();
					if (pstmt2 != null)
						pstmt2.close();
					if (resultset1 != null)
						resultset1.close();
					if (resultset2 != null)
						resultset2.close();
					if (connection != null)
						connection.close();
				} catch (Exception ex) {
					log.error("Error in closing resoures in getGraphstatistics:" + ex.getMessage());

				}
			}
		}
		return list;
	}
}
