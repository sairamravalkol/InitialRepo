/**
 * 
 */
package tvg.hpl.hp.com.services;

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
import tvg.hpl.hp.com.util.ApplicationConstants;
import tvg.hpl.hp.com.util.ManageAppProperties;

/**
 * @author sarmaji
 *
 */
public class ExtractSubGraph {
	static Logger log = LoggerFactory.getLogger(GetSubGraph.class);
	Map<String, List<List<String>>> subGraph = new HashMap<>();
	Set<String> vertexSet = new HashSet<String>();
	private String schema, query_table, result_table, fetch_size;
	private int insert_record_count;
	private StartBfsBean startBfsBean;
	private String taskId;
	ExecutorService executorService = Executors.newFixedThreadPool(2);

	public ExtractSubGraph(String taskIdPara) {
		try {
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
	
	public Map<String, List<List<String>>> getComputedSubGraph() {
		
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
			log.error("getComputedSubGraph : Computation Error retriving results from Future :" + e.getMessage());
		}
		return subGraph;
	}
	
}
