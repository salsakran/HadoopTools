package com.whitelabellabs.collector;


  
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
  

public class WLLCollector
{
	// do keytool -genkey -keystore /usr/local/wll-collector/keystore -keyalg RSA
	// this creates an unsigned certificate. provides privacy but not secure against impersonation of the collecting server
	static final String keystore_location = "/usr/local/wll-collector/keystore";
	static final String keystore_password = "123456";
	static final String ssl_password = "123456";
	
	public static void main(String[] args) throws Exception
	{
		Server server = new Server();
		 
		// Use the below sans ssl?
        SelectChannelConnector connector0 = new SelectChannelConnector();
        connector0.setPort(8080);
        server.addConnector(connector0);
		
        // SslSelectChannelConnector ssl_connector = new SslSelectChannelConnector();
        // ssl_connector.setPort(8443);
        // ssl_connector.setKeystore(keystore_location);
        // ssl_connector.setPassword(keystore_password);
        // ssl_connector.setKeyPassword(ssl_password);
        // server.addConnector(ssl_connector);
        
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        context.setContextPath("/");
        server.setHandler(context);
        
        context.addServlet(new ServletHolder(new HealthServlet()),"/health");
        context.addServlet(new ServletHolder(new BareCollectorServlet()),"/log");
        
        server.start();
        server.join();
        
	}
}
