package it.polimi.deib.city_sensing_server.linked_venues;

import it.polimi.deib.city_sensing_server.configuration.Config;
import it.polimi.deib.city_sensing_server.linked_venues.utilities.SimplifiedVenue;
import it.polimi.deib.city_sensing_server.utilities.GeneralUtilities;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.datatype.DatatypeConfigurationException;

import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.engine.header.Header;
import org.restlet.representation.Representation;
import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;
import org.restlet.util.Series;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.Syntax;


public class VenuesSADataServer extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(VenuesSADataServer.class.getName());
	
	private long actualizationInterval = 30495300000L;

	@SuppressWarnings({ "unchecked", "rawtypes"})
	@Post
	public void dataServer(Representation rep) throws IOException {

		Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().serializeNulls().create();
		Query query = null;
		QueryExecution qexec = null;

		try {

			logger.debug("Venue Social Activity request received");
			String parameters = rep.getText();
			logger.debug("parameters: {}",parameters);

			Series<Header> responseHeaders = (Series<Header>) getResponse().getAttributes().get("org.restlet.http.headers");
			if (responseHeaders == null) {
				responseHeaders = new Series(Header.class);
				getResponse().getAttributes().put("org.restlet.http.headers", responseHeaders);
			}
			responseHeaders.add(new Header("Access-Control-Allow-Origin", "*"));

			JsonReader reader = null;

			reader = new JsonReader(new StringReader(parameters));

			VenuesSARequest vReq = gson.fromJson(reader, VenuesSARequest.class);

			String venueListString = new String();

			if(vReq.getStart() == null || Long.parseLong(vReq.getStart()) < 0){
				vReq.setStart(String.valueOf(Long.parseLong(Config.getInstance().getDefaultStart()) + actualizationInterval));
			}
			if(vReq.getEnd() == null || Long.parseLong(vReq.getEnd()) < 0){
				vReq.setEnd(String.valueOf(Long.parseLong(Config.getInstance().getDefaultEnd()) + actualizationInterval));
			}
			if(vReq.getVenues() != null || vReq.getVenues().size() != 0){
				for(String s : vReq.getVenues()){
					venueListString = venueListString + s + ",";
				}
				venueListString = venueListString.substring(0, venueListString.lastIndexOf(","));
			}

			String sparqlQuery = "PREFIX prov:<http://www.w3.org/ns/prov#> "
					+ "PREFIX cse:<http://www.citydatafusion.org/ontologies/2014/1/cse#> "
					+ "PREFIX sma:<http://www.citydatafusion.org/ontologies/2014/1/sma#> "
					+ "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#> "
					+ "PREFIX sioc:<http://rdfs.org/sioc/ns#> "
					+ "PREFIX geo:<http://www.w3.org/2003/01/geo/wgs84_pos#> "
					+ "SELECT ?venue ?totalCount "
					+ "WHERE { "
					+ "{ SELECT ?venue (COUNT(DISTINCT ?mp) AS ?totalCount) "
					+ "  WHERE { "
					+ "		{ ?g prov:generatedAtTime ?graphGenTS . "
					+ "	 		FILTER(?graphGenTS >= \"" + GeneralUtilities.getXsdDateTime(Long.parseLong(vReq.getStart())) + "\"^^xsd:dateTime && ?graphGenTS <= \"" + GeneralUtilities.getXsdDateTime(Long.parseLong(vReq.getEnd())) + "\"^^xsd:dateTime) "
					+ "		} "
					+ "		{GRAPH ?g { "
					+" 			?mp sioc:topic ?venue . "
					+ "			FILTER (REGEX(xsd:string(?venue),'venue','i')) "
					+ "			} "
					+ "		} "
					+ "	FILTER(xsd:long(substr(xsd:string(?venue),62)) IN (" + venueListString + ")) "
					+ "	} GROUP BY ?venue "
					+ "} "
					+ "} "
					+ "ORDER BY DESC(?totalCount) ";

			long startTs = System.currentTimeMillis();
			query = QueryFactory.create(sparqlQuery,Syntax.syntaxSPARQL_11);
			qexec = QueryExecutionFactory.createServiceRequest(Config.getInstance().getTweetsSparqlEndpointURL(), query);

			ResultSet rs = qexec.execSelect();

			VenuesSAResponse response = new VenuesSAResponse();
			SimplifiedVenue sv;
			String venueStr = new String();

			while (rs.hasNext()) {
				QuerySolution qs = (QuerySolution) rs.next();
				if(qs.contains("venue")){
					sv = new SimplifiedVenue();
					venueStr = qs.getResource("venue").toString();
					sv.setId(venueStr.substring(venueStr.lastIndexOf("/") + 1, venueStr.length()));
					sv.setSocialActivity(Long.parseLong(qs.getLiteral("totalCount").getLexicalForm()));

					response.addElementToList(sv);
				}
			}


			long endTs = System.currentTimeMillis();

			logger.debug("Venue Social Activity query done, time: {} ms",endTs - startTs);

			startTs = System.currentTimeMillis();
			String respString = gson.toJson(response);
			endTs = System.currentTimeMillis();

			logger.debug("Venue Social Activity json serialization done, time: {} ms",endTs - startTs);

			rep.release();
			this.getResponse().setStatus(Status.SUCCESS_CREATED);
			this.getResponse().setEntity(respString, MediaType.APPLICATION_JSON);
			this.getResponse().commit();
			this.commit();	
			this.release();

		} catch (NumberFormatException e) {
			rep.release();
			String error = "Error while connecting to mysql DB and while closing resultset";
			logger.error(error, e);
			this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, error);
			this.getResponse().setEntity(gson.toJson(error), MediaType.APPLICATION_JSON);
			this.getResponse().commit();
			this.commit();	
			this.release();		
		} catch (DatatypeConfigurationException e) {
			rep.release();
			String error = "Error while connecting to mysql DB and while closing resultset";
			logger.error(error, e);
			this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, error);
			this.getResponse().setEntity(gson.toJson(error), MediaType.APPLICATION_JSON);
			this.getResponse().commit();
			this.commit();	
			this.release();		
		} finally {
			if(qexec != null)
				qexec.close();
		}
	}

}
