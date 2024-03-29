package tvg.hpl.hp.com.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tvg.hpl.hp.com.bean.StartBfsBean;
import tvg.hpl.hp.com.util.ApplicationConstants;
import tvg.hpl.hp.com.util.DataBaseUtility;

public class HopTwoDao implements Runnable{
	static Logger log = LoggerFactory.getLogger(HopTwoDao.class);
	private BasicDataSource datasource;
	private String query_table, result_table,test_table;
	private StartBfsBean startBfsBean;
	private String taskId;
	int[] insert_record_count;
	
	public HopTwoDao(StartBfsBean startBfsBeanPara, String taskIdPara) {
		this.startBfsBean = startBfsBeanPara;
		this.taskId = taskIdPara;
	}

	public HopTwoDao(StartBfsBean startBfsBeanPara, String taskIdPara, String querytable, String resulttable, String testtable) {
		startBfsBean = startBfsBeanPara;
		taskId = taskIdPara;
		query_table = querytable;
		result_table = resulttable;
		test_table = testtable;
	}

	public int findHopTwoNeighbours() {
		log.info("Find HOP Two Neighbours and Store in Table");
		String vertices = startBfsBean.getVertices();
		String start_time = startBfsBean.getStartTime();
		String end_time = startBfsBean.getEndTime();

		if (vertices != null && vertices != "" && start_time != null && start_time != "" && end_time != null
				&& end_time != "") {

			Connection conn = null;
			ResultSet resultset = null;
			PreparedStatement pstmt = null;
			Statement stmt = null;
			Set<Long> neighbourset = null;

			/**
			 * First get Hop One neighbour.
			 * 
			 */
		//	log.info("Hop Two is calling findHopOneNeighbours");
			new HopOneDao(startBfsBean, taskId, query_table, result_table,test_table).findHopOneNeighbours();
			try {
			//	System.out.println("Hop2 thread");
				datasource = DataBaseUtility.getVerticaDataSource();
				conn = datasource.getConnection();
				conn.setAutoCommit(false);
				neighbourset = new HashSet<Long>();
				try {
					String truncate = "TRUNCATE TABLE tvg4tm.K2";
					conn.prepareStatement(truncate).execute();
				} catch (Exception ex) {
					log.error("Error in Truncating tvg4tm.K2 Table in findHopTwoNeighbours()");
					conn.rollback();
				}
				String query = " SELECT  DISTINCT destination AS k2_neighbours FROM " + query_table
						+ " ,tvg4tm.K1 AS k1_neighbours " + " WHERE source = k1_neighbours.neighbour AND epoch_time >="
						+ start_time + " AND epoch_time <=" + end_time + " AND source not in(" + vertices
						+ " )  UNION SELECT DISTINCT source AS k2_neighbours FROM " + query_table
						+ " , tvg4tm.K1 AS k1_neighbours "
						+ " WHERE destination = k1_neighbours.neighbour AND epoch_time >= " + start_time
						+ " AND epoch_time <=" + end_time + " AND destination not in (" + vertices+ " ) ";
						
				//log.info("Find Hop Two Neighbours Query:" + query);
				pstmt = conn.prepareStatement(query);
				resultset = pstmt.executeQuery();
				while (resultset.next()) {
					neighbourset.add(resultset.getLong("k2_neighbours"));
				}
				pstmt.close();
				pstmt = conn.prepareStatement("INSERT INTO tvg4tm.K2 (k2_neighbours) VALUES(?)");
				for (Long i : neighbourset) {
					pstmt.setLong(1, i);
					pstmt.addBatch();
				}
				try {
					insert_record_count = pstmt.executeBatch();
				} catch (SQLException e) {
					log.error(
							"Error in inserting in tvg4tm.K2 table in findHopTwoNeighbours() method: pstmt.executeBatch():"
									+ e.getMessage());
					conn.rollback();
				}
				conn.commit();
			//	log.info("Size of HopTwo Neighbours :"+neighbourset.size());
			} catch (SQLException e) {
				try {
					conn.rollback();
				} catch (SQLException e1) {
					log.error("Error in roll back in findHopTwoNeighbours()");
				}
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
					log.error("Error in closing connections in findHopTwoNeighbours() method:" + ex.getMessage());
				}
			}
			if (startBfsBean.getHop().equals(ApplicationConstants.TVG_HOP_THREE)) {
				HopTwoDao runnable = new HopTwoDao(startBfsBean, taskId, query_table, result_table, test_table);
				Thread thread = new Thread(runnable);
				thread.start(); 
				//this.storeHopTwoEdges(startBfsBean, taskId);
			}
		}
		return insert_record_count.length;
	}

	public int storeHopTwoEdges(StartBfsBean startBfsBean, String taskId) {
		log.info("Find Hope Two Edges and Store in Result Table");
		int returnval=0;
		Connection conn = null;
		ResultSet resultset = null;
		PreparedStatement pstmt = null;
		PreparedStatement pstmtInsert = null;
		try {
			datasource = DataBaseUtility.getVerticaDataSource();
			conn = datasource.getConnection();// get connection
			conn.setAutoCommit(false);
			Long source = 0L;
			Long destination = 0L;
			startBfsBean.getVertices();
			String start_time = startBfsBean.getStartTime();
			String end_time = startBfsBean.getEndTime();
			
			String query = " select distinct " + taskId + " as task_id, T.source,T.destination, max(T.epoch_time) as epoch_time  FROM "+ query_table +" T  "
					+ " Join tvg4tm.K1 K1 on T.source=K1.neighbour join  tvg4tm.K2 K2 on T.destination = K2.K2_neighbours "
					+ " where epoch_time>" + start_time + " and epoch_time<" + end_time
					+ " group by T.source, T.destination  "
					+ " Union select distinct " + taskId + " as task_id, T.source,T.destination,max(T.epoch_time) as epoch_time  FROM "+ query_table +" T "
					+ " Join tvg4tm.K2 K2 on T.source= K2.K2_neighbours  join  tvg4tm.K1 K1 on T.destination =  K1.neighbour"
					+ " where epoch_time>" + start_time + " and epoch_time<" + end_time
					+ "  group by T.source,T.destination"; // limit 10";
			
		//	log.info("Find Hop Two Edges Query:" + query);
			String insert_query = " INSERT INTO "+result_table+" (task_id,source,destination,epoch_time) "+ query;
			//		+ " VALUES(?,?,?,?)";
			//pstmt = conn.prepareStatement(query);
			pstmtInsert = conn.prepareStatement(insert_query);
			 returnval=pstmtInsert.executeUpdate();
			/*resultset = pstmt.executeQuery();
			while (resultset.next()) {
				if (resultset.getLong("source") > resultset.getLong("destination")) {
					destination = resultset.getLong("source");
					source = resultset.getLong("destination");
				} else {
					source = resultset.getLong("source");
					destination = resultset.getLong("destination");
				}
				// System.out.println("source:" + source + "dest :" +
				// destination + " epoch time:" +
				// resultset.getLong("epoch_time"));

				*//**
				 * Insert subGraph in ws_result table
				 **
				 *//*
				pstmtInsert.setLong(1, Long.parseLong(taskId));
				pstmtInsert.setLong(2, source);
				pstmtInsert.setLong(3, destination);
				pstmtInsert.setLong(4, resultset.getLong("epoch_time"));
				pstmtInsert.addBatch();
			}
			try {
				insert_record_count = pstmtInsert.executeBatch();
			} catch (SQLException ex) {
				log.error("Error executing sql queries in storeHopTwoEdges, roll back");
				conn.rollback();
			}*/
			conn.commit();
			log.info("Hop2 edges Found:" + insert_record_count.length);
			// System.out.println("subgraph map :"+subGraph);
		} catch (Exception e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				log.error("Error in roll back in storeHopTwoEdges");
			}
		} finally {
			try {
				if (pstmt != null) {
					pstmt.close();
				}
				if (conn != null) {
					conn.close();
				}
				if (resultset != null) {
					resultset.close();
				}
			} catch (Exception ex) {
				log.error("Error in closing connections in HopTwo:storeHopTwoEdges method" + ex.getMessage());
			}
		}

		//return insert_record_count.length;
		return  returnval;
	}
	public int storeHopTwoEdgesModified(StartBfsBean startBfsBean, String taskId) {
		log.info("Find Hope Two Edges and Store in Result Table Modified version taskID :"+taskId);
		System.out.println("Find Hope Two Edges and Store in Result Table hopOneObj Modified version");
		String vertices = startBfsBean.getVertices();
		String start_time = startBfsBean.getStartTime();
		String end_time = startBfsBean.getEndTime();
		if (vertices != null && vertices != "" && start_time != null && start_time != "" && end_time != null
				&& end_time != "") {	
		Connection conn = null;
		ResultSet resultset = null;
		Statement stmt = null;
		PreparedStatement pstmt = null;
		PreparedStatement pstmtInsert = null;
		try {
			datasource = DataBaseUtility.getVerticaDataSource();
			conn = datasource.getConnection();
			conn.setAutoCommit(false);
			stmt = conn.createStatement();
			Long source = 0L;
			Long destination = 0L;
			
			String hopOneQuery = "select  distinct source,destination,max(epoch_time) as max_epoch_time FROM "+ query_table
					+ " INNER JOIN " +test_table +" ON "+ query_table+".destination="+test_table+".start_vertex "
					+ "	WHERE "+test_table+".start_vertex="+vertices +" AND "+ query_table+".epoch_time>='" + start_time + "' "
					+ " AND "+ query_table+".epoch_time<='" + end_time + "' GROUP BY "+ query_table+".source,"+ query_table+".destination "
					+ " UNION select distinct source,destination,max(epoch_time) as max_epoch_time FROM "+ query_table+ " INNER JOIN "+ test_table
					+ " ON "+ query_table+".source="+test_table+".start_vertex WHERE "+test_table+".start_vertex="+ vertices
					+ " AND "+ query_table+".epoch_time>='" + start_time + "' AND "+ query_table+".epoch_time<='" + end_time + "' "
					+ " GROUP BY "+ query_table+".source,"+ query_table+".destination ";
						
			String hopTwoQuery = "select distinct source,destination,max(epoch_time) as max_epoch_time FROM "+ query_table +" where destination "
					+ " IN (select distinct source AS Neighbor FROM "+ query_table
					+ " INNER JOIN "+ test_table + " ON " + query_table+".destination="+ test_table+".start_vertex "
					+ " WHERE "+test_table+".start_vertex in ("+vertices +") AND "+ query_table+".epoch_time>='" + start_time + "'"
					+ " AND "+ query_table+".epoch_time<='" + end_time + "' UNION select distinct destination "
					+ " AS Neighbor FROM "+ query_table +" INNER JOIN "+ test_table + " ON "+ query_table +".source="+test_table+".start_vertex "
					+ " WHERE "+test_table+".start_vertex IN ("+vertices +") AND "+ query_table+".epoch_time>="+start_time
					+ " AND "+ query_table+".epoch_time<="+end_time+") AND "+ query_table+".epoch_time>="+start_time
					+ " AND "+ query_table+".epoch_time<="+end_time +" AND source NOT IN ("+vertices+")"
					+ " GROUP BY "+ query_table+".source,"+ query_table+".destination "
					+ " UNION select distinct source,destination,max(epoch_time) FROM "+ query_table +" where source IN (select source AS Neighbor "
					+ " FROM "+ query_table+" INNER JOIN "+test_table+" ON "+ query_table+".destination="+test_table+".start_vertex "
					+ " WHERE "+test_table+".start_vertex IN ("+vertices +") AND "+ query_table+".epoch_time>="+start_time
					+ " AND "+ query_table+".epoch_time<="+end_time +" UNION select distinct destination "
					+ " AS Neighbor FROM "+ query_table+" INNER JOIN "+test_table+" ON "+ query_table+".source="+test_table+".start_vertex "
					+ " WHERE "+test_table+".start_vertex IN ("+ vertices +")  AND "+ query_table+".epoch_time>="+start_time +" AND "+ query_table+".epoch_time<="+end_time +")"
					+ " AND "+ query_table+".epoch_time>="+start_time +" AND "+ query_table+".epoch_time<="+end_time
					+ " AND destination NOT IN ( " +vertices+" ) GROUP BY "+ query_table+".source,"+ query_table+".destination";
			
			String query = hopOneQuery + " UNION " + hopTwoQuery; 
			System.out.println(query);
			//log.info(query);
			String insert_query = " INSERT INTO "+result_table+" (task_id,source,destination,epoch_time)"
					+ " VALUES(?,?,?,?)";
			pstmt = conn.prepareStatement(query);
			pstmtInsert = conn.prepareStatement(insert_query);
			resultset = pstmt.executeQuery();
			while (resultset.next()) {
				if (resultset.getLong("source") < resultset.getLong("destination")) {
					destination = resultset.getLong("source");
					source = resultset.getLong("destination");
				} else {
					source = resultset.getLong("source");
					destination = resultset.getLong("destination");
				}
			//	log.info("source:" + source + "dest :  " + destination + " epoc time:" + resultset.getLong("max_epoch_time"));

				/**
				 * Insert subGraph in ws_result table
				 **
				 */
				pstmtInsert.setLong(1, Long.parseLong(taskId));
				pstmtInsert.setLong(2, source);
				pstmtInsert.setLong(3, destination);
				pstmtInsert.setLong(4, resultset.getLong("max_epoch_time"));
				pstmtInsert.addBatch();
			}
			insert_record_count = pstmtInsert.executeBatch();
			conn.commit();
			log.info("Modified Hop2 Query Result Table : Number of records inserted:" + insert_record_count.length);
		} catch (Exception e) {
			try {
				conn.rollback();
				log.error("Error executing the Sql queries in storeHopOneEdges, rollback:"+e.getMessage());
				e.printStackTrace();
			} catch (SQLException e1) {
				log.error("Error in roll back in storeHopOneEdges:"+e1.getMessage());
			}
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
				log.error("Error in closing connections in storeHopOneEdges method" + ex.getMessage());
			}
		}
		}
		return insert_record_count.length;
	}

	@Override
	public void run() {
		this.storeHopTwoEdges(startBfsBean, taskId);
	}
}