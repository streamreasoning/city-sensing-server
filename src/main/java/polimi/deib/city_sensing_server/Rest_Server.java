package polimi.deib.city_sensing_server;

import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Restlet;
import org.restlet.data.Protocol;
import org.restlet.routing.Router;
import org.restlet.routing.Template;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import polimi.deib.city_sensing_server.concept_flows.ConceptFlowsDataServer;
import polimi.deib.city_sensing_server.concept_network.ConceptNetDataServer;
import polimi.deib.city_sensing_server.configuration.Config;
import polimi.deib.city_sensing_server.event.EventListDataServer;
import polimi.deib.city_sensing_server.map.MapDataServer;
import polimi.deib.city_sensing_server.side_panel.SidePanelDataServer;
import polimi.deib.city_sensing_server.timeline.ContextTimelineDataServer;
import polimi.deib.city_sensing_server.timeline.FocusTimelineDataServer;


public class Rest_Server extends Application {

	private static Logger logger = LoggerFactory.getLogger(Rest_Server.class.getName());
	private static String version = Config.getInstance().getServerVersion();

	public static void main(String[] args) throws Exception{

		Component component = new Component();
		component.getServers().add(Protocol.HTTP, Config.getInstance().getServerPort());

		component.getContext().getParameters().add("maxThreads", "512");
		component.getContext().getParameters().add("minThreads", "30");
		component.getContext().getParameters().add("lowThreads", "145");
		component.getContext().getParameters().add("maxQueued", "100");
		component.getContext().getParameters().add("maxTotalConnections", "100");
		component.getContext().getParameters().add("maxIoIdleTimeMs", "20000");
		component.getContext().getParameters().add("maxThreadIdleTimeMs", "60000");

		//		component.getServers().getContext().getParameters().add("maxThreads", "512");
		//		component.getServers().getContext().getParameters().add("minThreads", "30");
		//		component.getServers().getContext().getParameters().add("lowThreads", "145");
		//		component.getServers().getContext().getParameters().add("maxQueued", "100");
		//		component.getServers().getContext().getParameters().add("maxTotalConnections", "100");
		//		component.getServers().getContext().getParameters().add("maxIoIdleTimeMs", "100");
		//		component.getServers().getContext().getParameters().add("minThreads", "20"); 
		//		component.getServers().getContext().getParameters().add("lowThreads", "80"); 
		//		component.getServers().getContext().getParameters().add("maxThreads", "100"); 
		//		component.getServers().getContext().getParameters().add("maxQueued", "10"); 
		component.getClients().add(Protocol.FILE);  

		Rest_Server server = new Rest_Server();
		component.getDefaultHost().attach("", server);

		logger.debug("Starting city sensing Server with parameters : ");
		logger.debug("{}", component.getContext().getParameters().getFirst("maxThreads"));
		logger.debug("{}", component.getContext().getParameters().getFirst("minThreads"));
		logger.debug("{}", component.getContext().getParameters().getFirst("lowThreads"));
		logger.debug("{}", component.getContext().getParameters().getFirst("maxQueued"));
		logger.debug("{}", component.getContext().getParameters().getFirst("maxTotalConnections"));
		logger.debug("{}", component.getContext().getParameters().getFirst("maxIoIdleTimeMs"));
		logger.debug("{}", component.getContext().getParameters().getFirst("maxThreadIdleTimeMs"));

		component.getDefaultHost().attach(server); 

		try {
			component.start();
		} catch (Exception e) {
			logger.error("Error while starting city sensing server", e);
		}

	}

	public Restlet createInboundRoot(){

		Router router = new Router(getContext());
		router.setDefaultMatchingMode(Template.MODE_EQUALS);

		router.attach("/" + version + "/test",Test.class);
		router.attach("/" + version + "/map",MapDataServer.class);
		router.attach("/" + version + "/sidepanel",SidePanelDataServer.class);
		router.attach("/" + version + "/conceptnetwork",ConceptNetDataServer.class);
		router.attach("/" + version + "/conceptflows",ConceptFlowsDataServer.class);
		router.attach("/" + version + "/timeline/focus",FocusTimelineDataServer.class);
		router.attach("/" + version + "/timeline/context",ContextTimelineDataServer.class);
		router.attach("/" + version + "/eventlist",EventListDataServer.class);

		return router;
	}

}
