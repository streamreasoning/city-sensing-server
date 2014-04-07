package it.polimi.deib.city_sensing_server.linked_venues;

import it.polimi.deib.city_sensing_server.configuration.Config;
import it.polimi.deib.city_sensing_server.linked_venues.utilities.Venue;
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


public class VenuesTopDataServer extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(VenuesTopDataServer.class.getName());

	@SuppressWarnings({ "unchecked", "rawtypes"})
	@Post
	public void dataServer(Representation rep) throws IOException {

		Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().serializeNulls().create();
		Query query = null;
		QueryExecution qexec = null;

		try {

			logger.debug("Venues Top request received");
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

			VenuesTopRequest vReq = gson.fromJson(reader, VenuesTopRequest.class);

			String cellListString = new String();

			boolean allCells = false;

			if(vReq.getStart() == null || Long.parseLong(vReq.getStart()) < 0){
				vReq.setStart(String.valueOf(Long.parseLong(Config.getInstance().getDefaultStart())));
			}
			if(vReq.getEnd() == null || Long.parseLong(vReq.getEnd()) < 0){
				vReq.setEnd(String.valueOf(Long.parseLong(Config.getInstance().getDefaultEnd())));
			}
			if(vReq.getCells() == null || vReq.getCells().size() == 0){
				allCells = true;
			} else {
				for(String s : vReq.getCells()){
					cellListString = cellListString + "\"" + s + "\"" + "^^xsd:long,";
				}
				cellListString = cellListString.substring(0, cellListString.lastIndexOf(","));
			}
			if(vReq.getThreshold() == null){
				vReq.setThreshold(String.valueOf(100));
			}

			String sparqlQuery;

			if(allCells){
				sparqlQuery = "PREFIX prov:<http://www.w3.org/ns/prov#> "
						+ "PREFIX cse:<http://www.citydatafusion.org/ontologies/2014/1/cse#> "
						+ "PREFIX sma:<http://www.citydatafusion.org/ontologies/2014/1/sma#> "
						+ "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#> "
						+ "PREFIX sioc:<http://rdfs.org/sioc/ns#> "
						+ "PREFIX geo:<http://www.w3.org/2003/01/geo/wgs84_pos#> "
						+ "SELECT ?venue ?name ?lat ?long ?totalCount "
						+ "WHERE { "
						+ "{ SELECT ?venue (COUNT(DISTINCT ?mp) AS ?totalCount) "
						+ "		WHERE { "
						+ "			{ ?g prov:generatedAtTime ?graphGenTS . "
						+ "				 FILTER(?graphGenTS >= \"" + GeneralUtilities.getXsdDateTime(Long.parseLong(vReq.getStart())) + "\"^^xsd:dateTime && ?graphGenTS <= \"" + GeneralUtilities.getXsdDateTime(Long.parseLong(vReq.getEnd())) + "\"^^xsd:dateTime) "
						+ "			} "
						+ "			{GRAPH ?g { "
						+" 				?mp sioc:topic ?venue . "
						+ "				FILTER (REGEX(xsd:string(?venue),'venue','i')) "
						+ "				} "
						+ "			} "
						+ "			} GROUP BY ?venue  "
						+ "} "
						+ "{ SERVICE <http://www.streamreasoning.com/demos/mdw14/fuseki/cse_info/query> { "
						+ "		?venue cse:name ?name ; "
						+ "		geo:location ?l . "
						+ "		?l geo:lat ?lat ; "
						+ "		geo:long ?long . "
						//						+ "		OPTIONAL { "
						//						+ "			?venue sma:created_in ?cpRes . "
						//						+ "			?cpRes cse:city_pixel_ID ?cpId . "
						//						+ "		} "
						//						+ "		OPTIONAL { "
						//						+ "			?venue cse:city_pixel_ID ?cpId . "
						//						+ "		} "
						+ " }"
						//						+ "	FILTER(xsd:long(?cpId) > 0 && xsd:long(?cpId) < 10000) "
						+ "} "
						+ "}"
						+ "ORDER BY DESC(?totalCount) "
						+ "LIMIT " + vReq.getThreshold() + "";

			} else{

				sparqlQuery = "PREFIX prov:<http://www.w3.org/ns/prov#> "
						+ "PREFIX cse:<http://www.citydatafusion.org/ontologies/2014/1/cse#> "
						+ "PREFIX sma:<http://www.citydatafusion.org/ontologies/2014/1/sma#> "
						+ "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#> "
						+ "PREFIX sioc:<http://rdfs.org/sioc/ns#> "
						+ "PREFIX geo:<http://www.w3.org/2003/01/geo/wgs84_pos#> "
						+ "SELECT DISTINCT ?venue ?name ?lat ?long ?totalCount "
						+ "WHERE { "
						+ "{ SELECT ?venue (COUNT(?mp) AS ?totalCount) "
						+ "		WHERE { "
						+ "			{ ?g prov:generatedAtTime ?graphGenTS . "
						+ "			FILTER(?graphGenTS >= \"" + GeneralUtilities.getXsdDateTime(Long.parseLong(vReq.getStart())) + "\"^^xsd:dateTime && ?graphGenTS <= \"" + GeneralUtilities.getXsdDateTime(Long.parseLong(vReq.getEnd())) + "\"^^xsd:dateTime) "
						+ "			} "
						+ "			{GRAPH ?g { "
						+ "				?mp sioc:topic ?venue . "
						+ "				FILTER (REGEX(xsd:string(?venue),'venue','i')) " 
						+ "					} "
						+ "				} "
						+ "			} GROUP BY ?venue  "
						+ "} "
						+ "{ SERVICE <http://www.streamreasoning.com/demos/mdw14/fuseki/cse_info/query> { "
						+ "		?venue cse:name ?name ; "
						+ "		sma:created_in ?cpRes ; "
						+ "		geo:location ?l . "
						+ "		?l geo:lat ?lat ; "
						+ "		geo:long ?long . "
						//						+ "		OPTIONAL { "
						//						+ "			?venue sma:created_in ?cpRes . "
						//						+ "			?cpRes cse:city_pixel_ID ?cpId . "
						//						+ "		} "
						//						+ "		OPTIONAL { "
						//						+ "			?venue cse:city_pixel_ID ?cpId . "
						//						+ "		} "
						+ " FILTER(xsd:long(substr(xsd:string(?cpRes),59)) IN (" + cellListString + ")) "
						+ " }"
						+ "} "
						+ "}"
						+ "ORDER BY DESC(?totalCount) "
						+ "LIMIT " + vReq.getThreshold() + "";

			}

			long startTs = System.currentTimeMillis();
			query = QueryFactory.create(sparqlQuery,Syntax.syntaxSPARQL_11);
			qexec = QueryExecutionFactory.createServiceRequest(Config.getInstance().getTweetsSparqlEndpointURL(), query);

			ResultSet rs = qexec.execSelect();

			long endTs = System.currentTimeMillis();

			logger.debug("Venues top query done, time: {} ms",endTs - startTs);
			Venue venue;

			VenuesTopResponse response = new VenuesTopResponse();
			String venueStr;

			while (rs.hasNext()) {
				QuerySolution qs = (QuerySolution) rs.next();
				if(qs.contains("venue")){
					venue = new Venue();
					venueStr = qs.getResource("venue").toString();
					venue.setId(venueStr.substring(venueStr.lastIndexOf("/") + 1, venueStr.length()));
					venue.setName(qs.getLiteral("name").getLexicalForm());
					venue.setLatitude(Double.parseDouble(qs.getLiteral("lat").getLexicalForm()));
					venue.setLongitude(Double.parseDouble(qs.getLiteral("long").getLexicalForm()));
					venue.setSocialActivity(Long.parseLong(qs.getLiteral("totalCount").getLexicalForm()));
					response.addElementToList(venue);
				}
			}

			startTs = System.currentTimeMillis();
			String respString = gson.toJson(response);
			endTs = System.currentTimeMillis();

			logger.debug("User top json serialization done, time: {} ms",endTs - startTs);

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
			rep.release();
		}
	}
}
