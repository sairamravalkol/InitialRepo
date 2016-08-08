package tvg.hpl.hp.com.services;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import tvg.hpl.hp.com.bean.QueryResultBean;
import tvg.hpl.hp.com.bean.StartBfsBean;
import tvg.hpl.hp.com.dao.StartBfsDao;

public class GetNodeMetaData implements Runnable {
	static Map<String, List<String>> nodeMetaDataMap = new HashMap<>();
	private String taskId;
	public GetNodeMetaData(String taskIdPara){
		taskId = taskIdPara;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		StartBfsDao startbfsDao = new StartBfsDao();
		Set<String> vertices = startbfsDao.getAllVerticesByTaskId(taskId);
		StringBuffer bf = new StringBuffer();		
		for(String vertex :vertices){
			bf.append(vertex).append(",");			
		}
		bf.setLength(bf.length()-1);
		nodeMetaDataMap =	startbfsDao.getNodeMetaDataList(bf.toString());
	}
	
	public static Map<String, List<String>> getNodeMetaData(){
		return nodeMetaDataMap;
	}

}
