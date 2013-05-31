package eu.deib.polimi.city_sensing_server;

import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Restlet;
import org.restlet.data.Protocol;
import org.restlet.routing.Router;
import org.restlet.routing.Template;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.deib.polimi.city_sensing_server.concept_network.ConceptNetDataServer;
import eu.deib.polimi.city_sensing_server.configuration.Config;
import eu.deib.polimi.city_sensing_server.map_data_server.MapDataServer;
import eu.deib.polimi.city_sensing_server.side_panel_data_server.SidePanelDataServer;

public class Rest_Server extends Application {
	
	private static Logger logger = LoggerFactory.getLogger(Rest_Server.class.getName());

	public static void main(String[] args) throws Exception{

		Component component = new Component();
		component.getServers().add(Protocol.HTTP, Config.getInstance().getServerPort());
		component.getClients().add(Protocol.FILE);  

		Rest_Server server = new Rest_Server();
		component.getDefaultHost().attach("", server);

		component.getDefaultHost().attach(server); 

		try {
			component.start();
		} catch (Exception e) {
			logger.error("Error while starting Instagram Server", e);
		}

	}

	public Restlet createInboundRoot(){

		Router router = new Router(getContext());
		router.setDefaultMatchingMode(Template.MODE_EQUALS);

		router.attach("/test",Test.class);
		router.attach("/map",MapDataServer.class);
		router.attach("/sidepanel",SidePanelDataServer.class);
		router.attach("/conceptnetwork",ConceptNetDataServer.class);
//		router.attach("/focustimeline",FocusTimelineDataServer.class);
//		router.attach("/contexttimeline",ContextTimelineDataServer.class);

		return router;
	}

}
