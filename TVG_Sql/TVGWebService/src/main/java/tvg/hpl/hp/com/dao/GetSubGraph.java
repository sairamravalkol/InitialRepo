package tvg.hpl.hp.com.dao;

/**
 * @author sarmaji
 *
 */
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tvg.hpl.hp.com.bean.StartBfsBean;
import tvg.hpl.hp.com.util.ApplicationConstants;
import tvg.hpl.hp.com.util.ManageAppProperties;

public class GetSubGraph {

	static Logger log = LoggerFactory.getLogger(GetSubGraph.class);
	Map<String, List<List<String>>> subGraph = new HashMap<>();
	private String schema, query_table, result_table;
	private int[] insert_record_count;
	private StartBfsBean queryBean = new StartBfsBean();

	public GetSubGraph(StartBfsBean queryBeanpara) {
		try {
			ManageAppProperties instance = ManageAppProperties.getInstance();
			queryBean = queryBeanpara;
			Properties property = instance.getApp_prop();
			System.out.println(property);
			schema = property.getProperty(ApplicationConstants.DB_SCHEMA);
			query_table = property.getProperty(ApplicationConstants.QUERY_TABLE);
			result_table = property.getProperty(ApplicationConstants.RESULT_TABLE);
			StringBuilder sb = new StringBuilder();
			sb.append(schema).append(".").append(query_table);
			query_table = sb.toString();
			System.out.println(query_table);
			sb.setLength(0);
			sb.append(schema).append(".").append(result_table);
			result_table = sb.toString();		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Map<String, List<List<String>>> executeQuery() {
		if (queryBean.getHop().equals("1")) {
			log.debug("Hop1 start :");
			System.out.println("Hop1 Start:"+queryBean.getHop());
			
			/**
			 *  First Find Hop one neighbours and store in K1 Table.
			 */
			
			HopOne hopOneObj = new HopOne(queryBean, query_table, result_table);
			int[] no_of_hop_one_neighbours = hopOneObj.findHopOneNeighbours();
			if(no_of_hop_one_neighbours.length > 0)
			insert_record_count = hopOneObj.storeHopOneEdges(queryBean);
			if(insert_record_count.length > 0 ){
				StartBfsDao bfsDao = new StartBfsDao();
				subGraph = bfsDao.getSubgraphWithTaskId(queryBean);
			}

		} else if (queryBean.getHop().equals("2")) {
			log.debug("Hop2 start");
			
			/**
			 *  First Find Hop one neighbours and store in K1 Table.
			 */
			
			HopTwo hopTwoObj = new HopTwo(queryBean, query_table, result_table);
			int[]no_of_hop_two_neighbours = hopTwoObj.findHopTwoNeighbours();
			if(no_of_hop_two_neighbours.length > 0)
				insert_record_count = hopTwoObj.storeHopTwoEdges(queryBean);
			if(insert_record_count.length > 0 ){
				StartBfsDao bfsDao = new StartBfsDao();
				subGraph = bfsDao.getSubgraphWithTaskId(queryBean);
			}
			
		} else if (queryBean.getHop().equals("3")) {
			
			log.debug("HOP3 start");
			System.out.println("Hop3 Start");
			
			/**
			 *  First Find Hop Three neighbours and store in K1 Table.
			 */
			
			HopThree hopThreeObj = new HopThree(queryBean, query_table, result_table);
			int[]no_of_hop_three_neighbours = hopThreeObj.findHopThreeNeighbours();
			if(no_of_hop_three_neighbours.length > 0)
				insert_record_count = hopThreeObj.storeHopThreeEdges(queryBean);
			if(insert_record_count.length > 0 ){
				StartBfsDao bfsDao = new StartBfsDao();
				subGraph = bfsDao.getSubgraphWithTaskId(queryBean);
			}
		}
		return subGraph;
	}
}