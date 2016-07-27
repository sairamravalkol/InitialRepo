package tvg.hpl.hp.com.dao;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import tvg.hpl.hp.com.bean.StartBfsBean;

public class GetSubGraphResults implements Callable<Map<String, List<List<String>>>> {
	Map<String, List<List<String>>> subGraph = new HashMap<>();
	private StartBfsBean queryBean;

	public GetSubGraphResults(StartBfsBean bfsBean) {
		// TODO Auto-generated constructor stub
		queryBean = bfsBean;
	}
	
	@Override
	public Map<String, List<List<String>>> call() throws Exception {
		System.out.println("Executor service executing");
		StartBfsDao bfsDao = new StartBfsDao();
		subGraph = bfsDao.getSubgraphWithTaskId(queryBean);
		return subGraph;
	}

}
