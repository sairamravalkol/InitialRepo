package tvg.hpl.hp.com.dao;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import tvg.hpl.hp.com.bean.StartBfsBean;

public class GetNodeMetaData implements Runnable {
	static Map<String, List<String>> nodeMetaDataMap = new HashMap<>();
	private StartBfsBean queryBean;
	public GetNodeMetaData(StartBfsBean bfsBean){
		queryBean = bfsBean;		
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		StartBfsDao startbfsDao = new StartBfsDao();
		Set<String> vertices = startbfsDao.getAllVerticesByTaskId(queryBean);
		StringBuffer bf = new StringBuffer();		
		for(String vertex :vertices){
			bf.append(vertex).append(",");			
		}
		bf.setLength(bf.length()-1);
		System.out.println("Executor service:"+ bf.toString());
		nodeMetaDataMap =	startbfsDao.getNodeMetaDataList(bf.toString());
	}
	
	public static Map<String, List<String>> getNodeMetaData(){
		return nodeMetaDataMap;
	}

}
