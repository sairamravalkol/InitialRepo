package tvg.hpl.hp.com.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sarmaji
 *
 */
public class DataBaseUtility {
	final static Logger log = LoggerFactory.getLogger(DataBaseUtility.class);
	private static BasicDataSource datasource;

	private DataBaseUtility() {
		InputStream inputstream = null;
		if (datasource == null) {
			try {
				ClassLoader loader = Thread.currentThread().getContextClassLoader();
				Properties props = new Properties();
				inputstream = loader.getResourceAsStream("db.properties");
				props.load(inputstream);
				datasource = new BasicDataSource();
				datasource.setDriverClassName(props.getProperty(ApplicationConstants.DB_DRIVER));
				datasource.setUrl(props.getProperty(ApplicationConstants.DB_URL));
				datasource.setUsername(props.getProperty(ApplicationConstants.DB_USERNAME));
				datasource.setPassword(props.getProperty(ApplicationConstants.DB_PASSWORD));
				datasource.setMaxTotal(100);
			} catch (Exception ex) {
				log.error(ex.getMessage());
			} finally {
				try {
					inputstream.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					log.error(e.getMessage());
				}
			}
		}
	}

	public static BasicDataSource getVerticaDataSource() {
		if (datasource == null) {
			new DataBaseUtility();
		}
		return datasource;
	}
}
