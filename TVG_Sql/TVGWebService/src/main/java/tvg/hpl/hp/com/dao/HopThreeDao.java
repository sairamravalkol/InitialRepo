package tvg.hpl.hp.com.dao;

/**
 * @author sarmaji
 *
 */
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
import tvg.hpl.hp.com.util.DataBaseUtility;

public class HopThreeDao {
	static Logger log = LoggerFactory.getLogger(HopThreeDao.class);
	private BasicDataSource datasource;
	private String query_table, result_table,test_table;
	private StartBfsBean startBfsBean;
	private String taskId;
	private int[] insert_record_count;

	public HopThreeDao(StartBfsBean startBfsBeanPara, String taskIdPara, String querytable, String resulttable, String testtable) {
		this.startBfsBean = startBfsBeanPara;
		this.taskId = taskIdPara;
		this.query_table = querytable;
		this.result_table = resulttable;
		this.test_table = testtable;
	}

	public int findHopThreeNeighbours() {
		log.info("Find Hop Three Neighbours and Store in Table");
		System.out.println("Find Hop Three Neighbours");
		String vertices = startBfsBean.getVertices();
		String start_time = startBfsBean.getStartTime();
		String end_time = startBfsBean.getEndTime();
		Connection conn = null;
		ResultSet resultset = null;
		PreparedStatement pstmt = null;
		if (vertices != null && vertices != "" && start_time != null && start_time != "" && end_time != null
				&& end_time != "") {
			try {
				datasource = DataBaseUtility.getVerticaDataSource();
				conn = datasource.getConnection();
				Set<Long> neighbourset = new HashSet<Long>();
			//	log.info("Hop Three is calling findHopTwoNeighbours");
				new HopTwoDao(startBfsBean, taskId, query_table, result_table,test_table).findHopTwoNeighbours();
				conn.setAutoCommit(false);
				try {
					String truncate = "TRUNCATE TABLE tvg4tm.K3";
					conn.prepareStatement(truncate).execute();
				} catch (SQLException ex) {
					log.error("Error in truncating table tvg4tm.k3 :" + ex.getMessage());
					conn.rollback();
				}
				String query = " SELECT DISTINCT destination AS k3_neighbour FROM  " + query_table
						+ " ,TVG4TM.K2 AS k2_neighbours "
						+ " WHERE source = k2_neighbours.k2_neighbours AND epoch_time >=" + start_time
						+ " AND epoch_time <=" + end_time + " AND source not in (" + vertices
						+ " ) and source NOT IN (select distinct K1.neighbour FROM tvg4tm.K1)"
						+ " UNION SELECT DISTINCT source AS k3_neighbour FROM " + query_table
						+ " ,TVG4TM.K2 AS k2_neighbours "
						+ " WHERE destination = k2_neighbours.k2_neighbours AND epoch_time >=" + start_time
						+ " AND epoch_time <=" + end_time + " AND destination not in (" + vertices + ")"
						+ " and destination NOT IN (select distinct K1.neighbour FROM tvg4tm.K1)";
					
			/* " UNION SELECT DISTINCT k2_neighbours AS k3_neighbour FROM TVG4TM.K2 WHERE k2_neighbours not in ("
						+ vertices + ") AND k2_neighbours NOT IN (select distinct K1.neighbour FROM tvg4tm.K1)";*/
			//	log.info("Hop 3 Query:"+query);
				resultset = conn.prepareStatement(query).executeQuery();
				while (resultset.next()) {
					neighbourset.add(resultset.getLong("k3_neighbour"));
				}
				pstmt = conn.prepareStatement("INSERT INTO tvg4tm.K3 (k3_neighbours) VALUES(?)");
				for (Long i : neighbourset) {
					pstmt.setLong(1, i);
					pstmt.addBatch();
				}
				try {
					insert_record_count = pstmt.executeBatch();
					// endTime = System.currentTimeMillis();
				} catch (SQLException e) {
					log.error("Error message in inserting in tvg4tm.K3 table: pstmt.executeBatch() " + e.getMessage());
					conn.rollback();
				}
				conn.commit();
			//	log.info("Size of HopThree Neighbours :"+neighbourset.size());
			} catch (SQLException e) {
				log.error("Error in sql queries in findHopThreeNeighbours, roll back:" + e.getMessage());
				try {
					conn.rollback();
				} catch (SQLException e1) {
					log.error("Error in rollback in findHopThreeNeighbours :" + e1.getMessage());
				}
			} finally {
				try {
					if (pstmt != null) {
						pstmt.close();
					}
					if (resultset != null) {
						resultset.close();
					}
					if (conn != null) {
						conn.close();
					}
				} catch (Exception ex) {
					log.error("Error in closing connections in HopThree.findHopThreeNeighbours" + ex.getMessage());
				}
			}
		}
		return insert_record_count.length;

	}

	public int storeHopThreeEdges(StartBfsBean startBfsBean, String taskID) {
		log.info("Find Hope Three Edges and Store in Result Table");
		String vertices = startBfsBean.getVertices();
		String start_time = startBfsBean.getStartTime();
		String end_time = startBfsBean.getEndTime();
		Connection conn = null;
		ResultSet resultset = null;
		PreparedStatement pstmt = null;
		PreparedStatement pstmtInsert = null;
		if (vertices != null && vertices != "" && start_time != null && start_time != "" && end_time != null
				&& end_time != "") {
			try {
				datasource = DataBaseUtility.getVerticaDataSource();
				conn = datasource.getConnection();// get connection
				conn.setAutoCommit(false);
				Long source = 0L;
				Long destination = 0L;
				String query = " select distinct " + taskId + " as task_id, T.source,T.destination, max(T.epoch_time) as epoch_time  FROM "+ query_table +" T  "
						+ " INNER Join tvg4tm.K2 K2 on T.source=K2.K2_neighbours join  tvg4tm.K3 K3 on T.destination = K3.K3_neighbours "
						+ " where epoch_time>" + start_time + " and epoch_time<" + end_time
						+ " group by T.source, T.destination  "
						+ " Union select distinct " + taskId + " as task_id, T.source,T.destination,max(T.epoch_time) as epoch_time  FROM "+ query_table +" T "
						+ " INNER Join tvg4tm.K3 K3 on T.source= K3.K3_neighbours  join  tvg4tm.K2 K2 on T.destination =  K2.K2_neighbours"
						+ " where epoch_time>" + start_time + " and epoch_time<" + end_time
						+ " group by T.source,T.destination";
			//	System.out.println("HOP3 Query :" + query);
		//		log.info("HOP 3 Edges:"+query);
				
				String insert_query = " INSERT INTO "+result_table+" (task_id,source,destination,epoch_time) "+query;
				//		+ " VALUES(?,?,?,?)";
			//	pstmt = conn.prepareStatement(query);
				pstmtInsert = conn.prepareStatement(insert_query);
				pstmtInsert.executeUpdate();
				/*resultset = pstmt.executeQuery();
				while (resultset.next()) {
					if (resultset.getLong("source") < resultset.getLong("destination")) {
						destination = resultset.getLong("source");
						source = resultset.getLong("destination");
					} else {
						source = resultset.getLong("source");
						destination = resultset.getLong("destination");
					}
				//	System.out.println("source:" + source + "dest :" + destination + " epoch time:"+ resultset.getLong("epoch_time"));

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
				insert_record_count = pstmtInsert.executeBatch();*/
				conn.commit();			
			} catch (Exception e) {
				log.error("Error in executing Sql Queries in Hop3.storeHopThreeEdges, rollback:" + e.getMessage());
				try {
					conn.rollback();
				} catch (SQLException e1) {
					log.error("Error doing in roll back HopThree.storeHopThreeEdges:" + e1.getMessage());
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
					log.error("Error in closing connections in HopThree.storeHopThreeEdges method" + ex.getMessage());
				}
			}
		}
		return insert_record_count.length;
	}
	public int storeHopThreeEdgesModified(StartBfsBean startBfsBean, String taskId) {
		log.info("Find Hop Three Edges and Store in Result Table Modified version taskID :"+taskId);
		System.out.println("Find Hop Three Edges and Store in Result Table hop three Modified version");
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
									
			String hopThreeQuery = "select distinct source,destination,max(epoch_time) as max_epoch_time FROM "+ query_table 
					+ "	where destination IN (select distinct Source as Neighbor FROM "+ query_table
					+ " where destination IN (select distinct "+ query_table +".source AS Neighbor "
					+ " FROM "+ query_table +" INNER JOIN "+ test_table+" ON "+ query_table +".destination="+ test_table+".start_vertex"
					+ "	WHERE "+ test_table+".start_vertex="+vertices+" AND "+ query_table +".epoch_time>=" + start_time + " AND "+ query_table +".epoch_time<=" + end_time + " "
					+ " UNION select distinct "+ query_table +".destination AS Neighbor FROM "+ query_table +" INNER JOIN "+ test_table+" "
					+ "	ON "+ query_table +".source="+ test_table+".start_vertex WHERE "+ test_table+".start_vertex="+vertices+"  AND "+ query_table +".epoch_time>=" + start_time + " "
					+ "	AND "+ query_table +".epoch_time<=" + end_time + ") AND "+ query_table +".epoch_time>=" + start_time + " AND "+ query_table +".epoch_time<=" + end_time + " "
					+ "	AND source NOT IN ("+vertices+" ) "
					+ " UNION select distinct Destination as Neighbor FROM "+ query_table +"  where source IN (select distinct "+ query_table +".source AS Neighbor "
					+ " FROM "+ query_table +" INNER JOIN "+ test_table+ " ON "+ query_table +".destination="+ test_table+".start_vertex"
					+ "	WHERE "+ test_table+".start_vertex="+vertices+"  AND "+ query_table +".epoch_time>=" + start_time + " AND "+ query_table +".epoch_time<=" + end_time + " "
					+ " UNION select distinct "+ query_table +".destination AS Neighbor FROM "+ query_table +" INNER JOIN "+ test_table
					+ "	ON "+ query_table +".source="+ test_table+".start_vertex WHERE "+ test_table+".start_vertex="+vertices+"  AND "+ query_table +".epoch_time>=" + start_time + " "
					+ "	AND "+ query_table +".epoch_time<=" + end_time + ") AND "+ query_table +".epoch_time>=" + start_time + " AND "+ query_table +".epoch_time<=" + end_time + " "
					+ "	AND destination NOT IN ("+vertices+" )) AND "+ query_table +".epoch_time>=" + start_time + " AND "+ query_table +".epoch_time<=" + end_time + " "
					+ " AND source NOT IN ("+vertices+" )   AND source NOT IN (select distinct "+ query_table +".source AS Neighbor FROM "+ query_table +" INNER JOIN "+ test_table
					+ "	ON "+ query_table +".destination="+ test_table+".start_vertex WHERE "+ test_table+".start_vertex="+vertices+"  AND "
					+ " "+ query_table +".epoch_time>=" + start_time + " AND "+ query_table +".epoch_time<=" + end_time + " "
					+ " UNION select distinct "+ query_table +".destination AS Neighbor FROM "+ query_table +" INNER JOIN "+ test_table
					+ " ON "+ query_table +".source="+ test_table+".start_vertex WHERE "+ test_table+".start_vertex="+vertices+"  AND "+ query_table +".epoch_time>=" + start_time + " AND "+ query_table +".epoch_time<=" + end_time + ") "
					+ " GROUP BY "+ query_table +".source,"+ query_table +".destination"
					+ " UNION "
					+ "	select distinct "+ query_table +".source,"+ query_table +".destination,max("+ query_table +".epoch_time) as max_epoch_time"
					+ " FROM "+ query_table +" where source IN (select distinct Source as Neighbor FROM "+ query_table +" "
					+ " where destination IN (select distinct "+ query_table +".source AS Neighbor "
					+ " FROM "+ query_table +" INNER JOIN "+ test_table+" ON "+ query_table +".destination="+ test_table+".start_vertex "
					+ " WHERE "+ test_table+".start_vertex="+vertices+"  AND "+ query_table +".epoch_time>=" + start_time + " AND "+ query_table +".epoch_time<=" + end_time + " "
					+ " UNION select distinct "+ query_table +".destination AS Neighbor FROM "+ query_table +" INNER JOIN "+ test_table
					+ " ON "+ query_table +".source="+ test_table+".start_vertex WHERE "+ test_table+".start_vertex="+vertices+"  AND "+ query_table +".epoch_time>=" + start_time + " "
					+ " AND "+ query_table +".epoch_time<=" + end_time + ") AND "+ query_table +".epoch_time>=" + start_time + " AND "+ query_table +".epoch_time<=" + end_time + " "
					+ " AND source NOT IN ("+vertices+" ) UNION select distinct Destination as Neighbor FROM "+ query_table +"  "
					+ " where source IN (select distinct "+ query_table +".source AS Neighbor FROM "+ query_table +" "
					+ " INNER JOIN "+ test_table+" ON "+ query_table +".destination="+ test_table+".start_vertex WHERE "+ test_table+".start_vertex="+vertices+"  AND "+ query_table +".epoch_time>=" + start_time + " "
					+ " AND "+ query_table +".epoch_time<=" + end_time + " UNION select distinct "+ query_table +".destination AS Neighbor "
					+ " FROM "+ query_table +" INNER JOIN "+ test_table+" ON "+ query_table +".source="+ test_table+".start_vertex "
					+ " WHERE "+ test_table+".start_vertex="+vertices+"  AND "+ query_table +".epoch_time>=" + start_time + " AND "+ query_table +".epoch_time<=" + end_time + ") "
					+ " AND "+ query_table +".epoch_time>=" + start_time + " AND "+ query_table +".epoch_time<=" + end_time + " AND destination NOT IN ("+vertices+" ))"
					+ " AND "+ query_table +".epoch_time>=" + start_time + " AND "+ query_table +".epoch_time<=" + end_time + " AND destination NOT IN ("+vertices+" ) "
					+ " AND destination NOT IN (select distinct "+ query_table +".source AS Neighbor FROM "+ query_table +" "
					+ " INNER JOIN "+ test_table+" ON "+ query_table +".destination="+ test_table+".start_vertex WHERE "+ test_table+".start_vertex="+vertices+"  AND "
					+ " "+ query_table +".epoch_time>=" + start_time + " AND "+ query_table +".epoch_time<=" + end_time + " UNION  "
					+ " select distinct "+ query_table +".destination AS Neighbor FROM "+ query_table +" INNER JOIN "+ test_table+" "
					+ " ON "+ query_table +".source="+ test_table+".start_vertex WHERE "+ test_table+".start_vertex="+vertices+"  AND "
					+ " "+ query_table +".epoch_time>=" + start_time + " AND "+ query_table +".epoch_time<=" + end_time + ") "
					+ " GROUP BY "+ query_table +".source,"+ query_table +".destination ";
			
			//String query = hopOneQuery + " UNION " + hopTwoQuery + " UNION "+ hopThreeQuery; 
			String query = hopThreeQuery;
		//	System.out.println(query);
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
		//	log.info("Result Table : Number of records inserted:" + insert_record_count.length);
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
}
