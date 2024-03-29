package tvg.hpl.hp.com.servlets;

/**
 * @author sarmaji
 *
 */
import java.io.IOException;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebListener;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tvg.hpl.hp.com.util.DataBaseUtility;
import tvg.hpl.hp.com.util.ManageAppProperties;

/**
 * Servlet implementation class TVGServletContextListener
 */
//@WebServlet("/TVGServletContextListener")
@WebListener
public class TVGServletContextListener extends HttpServlet implements ServletContextListener {
	static Logger log = LoggerFactory.getLogger(TVGServletContextListener.class);
	private static final long serialVersionUID = 1L;

       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public TVGServletContextListener() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
     * @see ServletContextListener#contextInitialized(ServletContextEvent)
     */
    public void contextInitialized(ServletContextEvent sce)  { 
         // TODO Auto-generated method stub
    	log.info("ServletContextListener started");
    	/**
    	 * ManageAppProperties initializes the resources
    	 */
    	log.info("Initialize Application Property");
    	ManageAppProperties.getInstance();
    	/**
    	 * Create DatabaseConnection Pool
    	 * 
    	 */
    	log.info("Initialize Database Connection and create connection pooling");
    	BasicDataSource datasource = DataBaseUtility.getVerticaDataSource();
      }

	/**
     * @see ServletContextListener#contextDestroyed(ServletContextEvent)
     */
    public void contextDestroyed(ServletContextEvent sce)  { 
         // TODO Auto-generated method stub
    	log.info("ServletContextListener Destroyed");
    	Enumeration<Driver> drivers = DriverManager.getDrivers(); 
    	Driver driver = null;
    	// clear drivers
        while(drivers.hasMoreElements()) {
            try {
                driver = drivers.nextElement();
                DriverManager.deregisterDriver(driver);
               log.info("Unregistered JDBC driver");
            } catch (SQLException ex) {
                // deregistration failed, might want to do something, log at the very least
            	ex.printStackTrace();
            }
        }
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		response.getWriter().append("Served at: ").append(request.getContextPath());
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
