package tvg.hpl.hp.com.services;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tvg.hpl.hp.com.dao.StartBfsDao;

public class GetSubGraphResults implements Callable<Map<String, List<List<String>>>> {
	static Logger log = LoggerFactory.getLogger(GetSubGraphResults.class);
	Map<String, List<List<String>>> subGraph = new HashMap<>();
	private String taskId; 
	private String fetchSize;

	public GetSubGraphResults(String taskIdPara, String fetchSizePara) {
		this.taskId = taskIdPara;
		this.fetchSize = fetchSizePara;
	}
	
	@Override
	public Map<String, List<List<String>>> call() throws Exception {
		log.info("Executor Service Fetching Results from the Database");
		StartBfsDao startBfsDao = new StartBfsDao();
		subGraph = startBfsDao.getSubgraphWithTaskId(taskId, fetchSize);
		log.info("Result SubGraph Size:"+subGraph.size());
		return subGraph;
	}

}
