package com.hpl.hp.com.dao;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hpl.hp.com.bean.StartBfsBean;

import tvg.hpl.hp.com.util.DataBaseUtility;
import tvg.hpl.hp.com.util.ManageAppProperties;


public class GetSubGraph {

	static Logger log = LoggerFactory.getLogger(GetSubGraph.class);
	private final String DATABASE_SCHEMA = "SCHEMA";
	private final String TEST_TABLE = "TEST_TABLE";
	private final String QUERY_TABLE = "QUERY_TABLE";
	private String schema, test_table, query_table, result_table;
	StartBfsBean queryBean;

	public GetSubGraph(StartBfsBean queryBeanpara) {
		try {
			ManageAppProperties instance = ManageAppProperties.getInstance();
			queryBean = queryBeanpara;
			Properties property = instance.getApp_prop();
			System.out.println(property);
			schema = property.getProperty(DATABASE_SCHEMA);
			test_table = property.getProperty(TEST_TABLE);
			query_table = property.getProperty(QUERY_TABLE);
			System.out.println(query_table);

			StringBuilder sb = new StringBuilder();
			sb.append(schema).append(".").append(test_table);
			test_table = sb.toString();
			sb.setLength(0);
			sb.append(schema).append(".").append(result_table);
			result_table = sb.toString();
			sb.setLength(0);
			sb.append(schema).append(".").append(query_table);
			query_table = sb.toString();
			System.out.println(query_table);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void readTestTable() {
		Connection conn = null;
		ResultSet resultset = null;
		PreparedStatement pstmt = null;
		BasicDataSource datasource = null;
		Map<Integer, Map<Long, List<List<Long>>>> hopmap = new HashMap<>();
		try {
			datasource = DataBaseUtility.getVerticaDataSource();
			conn = datasource.getConnection();
			
				String query = "select distinct test_id,from_time,to_time,start_vertex,hop,idx "
						+ " from tvg4tm.regression_tests where from_time>="+queryBean.getStartTime() +" and to_time<="+queryBean.getEndTime()
						+ " and hop="+ queryBean.getHop() + " and start_vertex in ("+queryBean.getVertices() +") limit 1";
				System.out.println(query);
				pstmt = conn.prepareStatement(query);
				resultset = pstmt.executeQuery();
				while (resultset.next()) {
					List<Long> vertices = new ArrayList<>();
					List<Long> parameters = new ArrayList<>();
					List<List<Long>> hoplist = new ArrayList<>();
					Map<Long, List<List<Long>>> innerhopmap = new HashMap<>();

					if (hopmap.get((int) resultset.getLong("hop")) != null)
						innerhopmap = hopmap.get((int) resultset.getLong("hop"));
					// System.out.println("MAP="+innerhopmap);
					if ((hopmap.get((int) resultset.getLong("hop")) != null)
							&& (hopmap.get((int) resultset.getLong("hop")).containsKey(resultset.getLong("test_id")))) {

						vertices = hopmap.get((int) resultset.getLong("hop")).get(resultset.getLong("test_id")).get(0);
						vertices.add(resultset.getLong("start_vertex"));
						// System.out.println("vertices :"+vertices);
						hoplist.add(vertices);
						parameters = hopmap.get((int) resultset.getLong("hop")).get(resultset.getLong("test_id"))
								.get(1);
						// System.out.println("parameters="+parameters);
						if (resultset.getLong("idx") > parameters.get(2)) {
							parameters.remove(2);
							parameters.add(resultset.getLong("idx"));
						}
						hoplist.add(parameters);
						innerhopmap.put(resultset.getLong("test_id"), hoplist);

					} else {
						vertices.add(resultset.getLong("start_vertex"));
						hoplist.add(vertices);
						parameters.add(resultset.getLong("from_time"));
						parameters.add(resultset.getLong("to_time"));
						parameters.add(resultset.getLong("idx"));
						hoplist.add(parameters);
						innerhopmap.put(resultset.getLong("test_id"), hoplist);
					}
					hopmap.put((int) resultset.getLong("hop"), innerhopmap);
				}

				if (queryBean.getHop().equals("1")) {
					log.debug("Hop1 start :");
					System.out.println("Hop1 Start");
					new HopOne(hopmap.get(Integer.parseInt(queryBean.getHop())), false, datasource, query_table, result_table)
							.runHopOne();
					
				} else if (queryBean.getHop().equals("2")) {
					log.debug("Hop1 done");
					log.debug("Hop2 start");
					System.out.println("Hop2 Starts");
					new HopTwo(hopmap.get(Integer.parseInt(queryBean.getHop())), false, datasource, query_table, result_table)
							.runHopTwo();					
				} else if (queryBean.getHop().equals("3")) {
					log.debug("Hop2 end");
					log.debug("HOP3 start");
					System.out.println("Hop3 Start");					
					new HopThree(hopmap.get(Integer.parseInt(queryBean.getHop())), false, datasource, query_table, result_table)
					.runHopThree();				
				}
			
			
		} catch (SQLException e) {
			e.printStackTrace();
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
				log.error("Error in closing connections in TVGComparision.test_k_hop() method" + ex.getMessage());
			}
		}
	}

}
