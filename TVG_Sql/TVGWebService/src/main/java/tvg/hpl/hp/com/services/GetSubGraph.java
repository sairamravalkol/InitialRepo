package tvg.hpl.hp.com.services;

/**
 * @author sarmaji
 *
 */
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tvg.hpl.hp.com.bean.StartBfsBean;
import tvg.hpl.hp.com.dao.HopOneDao;
import tvg.hpl.hp.com.dao.HopThreeDao;
import tvg.hpl.hp.com.dao.HopTwoDao;
import tvg.hpl.hp.com.util.ApplicationConstants;
import tvg.hpl.hp.com.util.ManageAppProperties;
import tvg.hpl.hp.com.util.VertexTokenSet;

public class GetSubGraph {
	static Logger log = LoggerFactory.getLogger(GetSubGraph.class);
	Map<String, List<List<String>>> subGraph = new HashMap<>();
	Set<String> vertexSet = new HashSet<String>();
	private String schema, query_table, result_table, fetch_size;
	private int insert_record_count;
	private StartBfsBean startBfsBean;
	private String taskId;
	ExecutorService executorService = Executors.newFixedThreadPool(2);

	public GetSubGraph(StartBfsBean queryBeanPara, String taskIdPara) {
		try {
			startBfsBean = queryBeanPara;
			taskId = taskIdPara;
			ManageAppProperties instance = ManageAppProperties.getInstance();
			Properties property = instance.getApp_prop();
			schema = property.getProperty(ApplicationConstants.DB_SCHEMA);
			query_table = property.getProperty(ApplicationConstants.QUERY_TABLE);
			result_table = property.getProperty(ApplicationConstants.RESULT_TABLE);
			fetch_size = property.getProperty(ApplicationConstants.RECORDS_FETCH_SIZE);
			StringBuilder sb = new StringBuilder();
			sb.append(schema).append(".").append(query_table);
			query_table = sb.toString();
			sb.setLength(0);
			sb.append(schema).append(".").append(result_table);
			result_table = sb.toString();
		} catch (Exception e) {
			log.error("Error in GetSubGraph Constructor:" + e.getMessage());
		}
	}

	public Map<String, List<List<String>>> executeQuery() {

		if (startBfsBean.getHop().equals(ApplicationConstants.TVG_HOP_ONE)) {
			log.debug("Hop One is Executing :");
			System.out.println("Hop One is Executing:" + startBfsBean.getHop());

			/**
			 * First Find Hop one neighbors and store in K1 Table.
			 */
			vertexSet = new VertexTokenSet().getVerxtexSet(startBfsBean.getVertices());
			if(vertexSet.size() > 0 ) {
				for(String vertex : vertexSet) {
					startBfsBean.setVertices(vertex);	
			HopOneDao hopOneObj = new HopOneDao(startBfsBean, taskId, query_table, result_table);
			int no_of_hop_one_neighbours = hopOneObj.findHopOneNeighbours();
			if (no_of_hop_one_neighbours > 0) {
				insert_record_count = hopOneObj.storeHopOneEdges(startBfsBean, taskId);
				
			} else {
				log.info("Hop One Neighbours Not Found, return empty Subgraph");
				subGraph = new HashMap<String, List<List<String>>>();
			}
		}
				if (insert_record_count > 0) {
					Future<Map<String, List<List<String>>>> future = executorService
							.submit(new GetSubGraphResults(taskId, fetch_size));
					executorService.execute(new GetNodeMetaData(taskId));
					try {
						executorService.shutdown();
						executorService.awaitTermination(10, TimeUnit.MINUTES);
						subGraph = future.get();
					} catch (InterruptedException e) {
						log.error("Error in shutting down executor service:" + e.getMessage());
					} catch (ExecutionException e) {
						log.error("Hop One : Computation Error retriving results from Future :" + e.getMessage());
					}
				} else {
					log.error("Error Storing Hop One Edges");
				}
			}else{
				log.info("VertexSet is empty");
			}
			
			log.info("Hop One Execution Completed");

		} else if (startBfsBean.getHop().equals(ApplicationConstants.TVG_HOP_TWO)) {
			log.debug("Hop Two is Executing:");

			/**
			 * First Find Hop Two neighbors and store in K2 Table.
			 */

			HopTwoDao hopTwoObj = new HopTwoDao(startBfsBean, taskId, query_table, result_table);
			int no_of_hop_two_neighbours = hopTwoObj.findHopTwoNeighbours();
			if (no_of_hop_two_neighbours > 0) {
				insert_record_count = hopTwoObj.storeHopTwoEdges(startBfsBean, taskId);
				if (insert_record_count > 0) {
					Future<Map<String, List<List<String>>>> future = executorService
							.submit(new GetSubGraphResults(taskId, fetch_size));
					executorService.execute(new GetNodeMetaData(taskId));
					try {
						executorService.shutdown();
						executorService.awaitTermination(10, TimeUnit.MINUTES);
						try {
							subGraph = future.get();
						} catch (ExecutionException e) {
							log.error("Hop Two : Computation Error retriving results from Future :" + e.getMessage());
						}
					} catch (InterruptedException e) {
						log.error("Error in shutting down Executor Service in Hop Two:" + e.getMessage());
					}
				}
			} else {
				log.info("Empty Two Hop Neighbours");
				subGraph = new HashMap<String, List<List<String>>>();
			}
			log.info("Hop Two Execution Completed");
			
		} else if (startBfsBean.getHop().equals(ApplicationConstants.TVG_HOP_THREE)) {
			log.debug("HOP three is Executing");
			
			/**
			 * First Find Hop Three neighbours and store in K3 Table.
			 */

			HopThreeDao hopThreeObj = new HopThreeDao(startBfsBean, taskId, query_table, result_table);
			int no_of_hop_three_neighbours = hopThreeObj.findHopThreeNeighbours();
			if (no_of_hop_three_neighbours > 0) {
				insert_record_count = hopThreeObj.storeHopThreeEdges(startBfsBean, taskId);
				if (insert_record_count > 0) {
					Future<Map<String, List<List<String>>>> future = executorService
							.submit(new GetSubGraphResults(taskId, fetch_size));
					executorService.execute(new GetNodeMetaData(taskId));
					try {
						executorService.shutdown();
						executorService.awaitTermination(10, TimeUnit.MINUTES);
						subGraph = future.get();
					} catch (InterruptedException e) {
						log.error("Error in shutting down Executor Service in Hop Three:"+e.getMessage());

					} catch (ExecutionException e) {
						log.error("Hop Three : Computation Error retriving results from Future :"+e.getMessage());
					}
				}
			} else {
				log.info("Empty Three Hop Neighbours");
				subGraph = new HashMap<String, List<List<String>>>();
			}
		}
		return subGraph;
	}
}