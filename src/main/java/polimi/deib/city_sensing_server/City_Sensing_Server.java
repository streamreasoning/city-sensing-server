package polimi.deib.city_sensing_server;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

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
import polimi.deib.city_sensing_server.dataSource.DataSourceSingleton;
import polimi.deib.city_sensing_server.event.EventListDataServer;
import polimi.deib.city_sensing_server.map.MapDataServer;
import polimi.deib.city_sensing_server.most_contacted_chart.MostContactedChartDataServer;
import polimi.deib.city_sensing_server.side_panel.SidePanelDataServer;
import polimi.deib.city_sensing_server.timeline.ContextTimelineDataServer;
import polimi.deib.city_sensing_server.timeline.FocusTimelineDataServer;


public class City_Sensing_Server extends Application {

	private static Logger logger = LoggerFactory.getLogger(City_Sensing_Server.class.getName());
	private static String version = Config.getInstance().getServerVersion();

//	public static BasicDataSource bds;

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
				
//		bds = new BasicDataSource();
//		bds.setDriverClassName("com.mysql.jdbc.Driver");
//		bds.setUrl("jdbc:mysql://" + Config.getInstance().getMysqlAddress() + "/" + Config.getInstance().getMysqldbname());
//		bds.setUsername(Config.getInstance().getMysqlUser());
//		bds.setPassword(Config.getInstance().getMysqlPwd());
//		bds.setMaxActive(Config.getInstance().getMaxActiveConnectionNumber());
//		bds.setMaxIdle(Config.getInstance().getMaxIdleConnectionNumber());

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

		City_Sensing_Server server = new City_Sensing_Server();
		component.getDefaultHost().attach("", server);

		logger.debug("Starting city sensing Server with parameters : ");
		logger.debug("{}", component.getContext().getParameters().getFirst("maxThreads"));
		logger.debug("{}", component.getContext().getParameters().getFirst("minThreads"));
		logger.debug("{}", component.getContext().getParameters().getFirst("lowThreads"));
		logger.debug("{}", component.getContext().getParameters().getFirst("maxQueued"));
		logger.debug("{}", component.getContext().getParameters().getFirst("maxTotalConnections"));
		logger.debug("{}", component.getContext().getParameters().getFirst("maxIoIdleTimeMs"));
		logger.debug("{}", component.getContext().getParameters().getFirst("maxThreadIdleTimeMs"));
		
		logger.debug("User connected to database: {}", Config.getInstance().getMysqlUser());
		logger.debug("Create view on db: {}", Config.getInstance().getCreateViewOnDB());

		component.getDefaultHost().attach(server); 

		component.start();

		if(Config.getInstance().getCreateViewOnDB())
			createViews();

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
		router.attach("/" + version + "/inoutchart",MostContactedChartDataServer.class);

		return router;
	}

	private static void createViews() throws SQLException{

		Connection connection = null;
		PreparedStatement p1 = null;
		PreparedStatement p2 = null;
		PreparedStatement p3 = null;


		try{
			connection = DataSourceSingleton.getInstance().getConnection();
			connection.setAutoCommit(false);

			String sqlQuery = "CREATE OR REPLACE VIEW `foursquare_cat_in_square` AS " +
					"SELECT DISTINCT FOURSQUARE_CAT.name AS name, VENUE.venue_square_ID AS square_ID " +
					"FROM FOURSQUARE_CAT,VENUE_FOURSQUARE,VENUE " +
					"WHERE VENUE_FOURSQUARE.venue_fsq_ID = VENUE.venue_ID AND VENUE_FOURSQUARE.fsq_cat_ID = FOURSQUARE_CAT.fsq_cat_ID";

			p1 = connection.prepareStatement(sqlQuery);

			p1.executeUpdate();

			sqlQuery = "CREATE OR REPLACE VIEW `fuorisalone_cat_in_square` AS " +
					"SELECT DISTINCT FUORISALONE_CAT.name AS name, VENUE.venue_square_ID AS square_ID " +
					"FROM FUORISALONE_CAT,VENUE,EVENT " +
					"WHERE VENUE.venue_ID = EVENT.event_venue_ID AND " +
					"EVENT.event_fs_cat_ID = FUORISALONE_CAT.fs_cat_ID";

			p2 = connection.prepareStatement(sqlQuery);

			p2.executeUpdate();

			sqlQuery = "CREATE OR REPLACE VIEW `hashtag_in_square_by_ts` AS " +
					"SELECT DISTINCT hashtag, ht_square_ID, ht_ts_ID " +
					"FROM HASHTAG_SQUARE ";

			p3 = connection.prepareStatement(sqlQuery);

			p3.executeUpdate();

			connection.commit();



		} catch(SQLException e) { 
			connection.rollback();
		} catch (Exception e) {
			logger.error("Error while starting city sensing server", e);
		} finally {
			if(p1 != null){ p1.close();}
			if(p2 != null){ p2.close();}
			if(p3 != null){ p3.close();}
			if(connection != null){connection.close();}
		}
	}

}
