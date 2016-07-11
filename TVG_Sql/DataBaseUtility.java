package tvg.hpl.hp.com.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.commons.dbcp2.BasicDataSource;

public class DataBaseUtility
{
	private static BasicDataSource datasource;
	
	private DataBaseUtility(){
		InputStream inputstream = null;
		if (datasource == null) {
			try {
				ClassLoader loader = Thread.currentThread().getContextClassLoader();
				Properties props = new Properties();
				inputstream = loader.getResourceAsStream("db.properties") ;
				props.load(inputstream);
				System.out.println(props);
				datasource = new BasicDataSource();
				datasource.setDriverClassName(props.getProperty("VERTICA_DB_DRIVER_CLASS"));
				datasource.setUrl(props.getProperty("VERTICA_DB_URL"));
				datasource.setUsername(props.getProperty("VERTICA_DB_USERNAME"));
				datasource.setPassword(props.getProperty("VERTICA_DB_PASSWORD"));
				datasource.setMaxTotal(100);
				// datasource.setMinIdle(5);
				// datasource.setMaxIdle(10);
				//datasource.setMaxOpenPreparedStatements(1);
			} catch (Exception ex) {

			} finally {
				try {
					inputstream.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}
	
	public static BasicDataSource getVerticaDataSource(){
		
		if(datasource == null){
			new DataBaseUtility();
		}
		return datasource;
		
	}
	
}
