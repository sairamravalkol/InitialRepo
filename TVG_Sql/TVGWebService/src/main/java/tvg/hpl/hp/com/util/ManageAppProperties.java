package tvg.hpl.hp.com.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sarmaji
 *
 */
public class ManageAppProperties {
	static Logger log = LoggerFactory.getLogger(ManageAppProperties.class);
	private static ManageAppProperties instance;
	private Properties props;
	private InputStream inputstream;

	private ManageAppProperties() {
		// TODO Auto-generated constructor stub		
		try {
			ClassLoader loader = Thread.currentThread().getContextClassLoader();
			props = new Properties();
			inputstream = loader.getResourceAsStream("application.properties") ;
			props.load(inputstream);
			System.out.println("App properties:"+props);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			log.error("File Not Found in ManageAppProperties:"+e.getMessage());
			//e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if(inputstream !=null )
			{
				try {
					inputstream.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					//e.printStackTrace();
					log.error("Problem In closing InputStream:"+e.getMessage());
				}
			}
		}
	}
	public Properties getApp_prop() {
		return props;
	}

	public void setApp_prop(Properties app_prop) {
		this.props = app_prop;
	}
	public static ManageAppProperties getInstance(){
		if(instance == null){
			instance = new ManageAppProperties();
		}
		return instance;
		
	}
	
}
