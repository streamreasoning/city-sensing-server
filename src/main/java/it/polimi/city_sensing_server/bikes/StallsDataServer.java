package it.polimi.city_sensing_server.bikes;

import it.polimi.city_sensing_server.bikes.utilities.Stall;
import it.polimi.deib.city_sensing_server.configuration.Config;
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


public class StallsDataServer extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(StallsDataServer.class.getName());

	@SuppressWarnings({ "unchecked", "rawtypes"})
	@Post
	public void dataServer(Representation rep) throws IOException {

		Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().serializeNulls().create();
		Query query = null;
		QueryExecution qexec = null;

		try {

			logger.debug("Stalls request received");
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

			StallsRequest sReq = gson.fromJson(reader, StallsRequest.class);

			String cellListString = new String();

			boolean allCells = false;

			if(sReq.getWhen() == null || Long.parseLong(sReq.getWhen()) < 0){
				sReq.setWhen(String.valueOf(Long.parseLong(Config.getInstance().getDefaultStart())));
			}
			if(sReq.getCells() == null || sReq.getCells().size() == 0){
				allCells = true;
			} else {
				for(String s : sReq.getCells()){
					cellListString = cellListString + s + ",";
				}
				cellListString = cellListString.substring(0, cellListString.lastIndexOf(","));
			}

			String sparqlQuery;

			if(allCells){
				sparqlQuery = "PREFIX prov:<http://www.w3.org/ns/prov#> "
						+ "PREFIX cse:<http://www.citydatafusion.org/ontologies/2014/1/cse#> "
						+ "PREFIX sma:<http://www.citydatafusion.org/ontologies/2014/1/sma#> "
						+ "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#> "
						+ "PREFIX sioc:<http://rdfs.org/sioc/ns#> "
						+ "PREFIX geo:<http://www.w3.org/2003/01/geo/wgs84_pos#> "
						+ "SELECT ?id ?name ?ab ?as ?lat ?long "
						+ "WHERE { "
						+ "{ ?g prov:generatedAtTime ?graphGenTS . "
						+ "FILTER(?graphGenTS = \"" + GeneralUtilities.getXsdDateTime(Long.parseLong(sReq.getWhen())) + "\"^^xsd:dateTime) "
						+ "} "
						+ "{GRAPH ?g { "
						+ "		?stall cse:id ?id ; "
						+ "		cse:name ?name ; "
						+ "		cse:availableBikes ?ab ; "
						+ "		cse:availbaleStalls ?as ; "
						+ "		sma:created_in ?cp ; "
						+ "		geo:location ?l . "
						+ "		?l geo:lat ?lat ; "
						+ "		geo:long ?long . "
						+ "		FILTER(xsd:integer(substr(xsd:string(?cp),59)) > \"0\"^^xsd:integer && xsd:integer(substr(xsd:string(?cp),59)) < \"10000\"^^xsd:integer) "
						+ "	} "
						+ "} "
						+ "}";

			} else{
				sparqlQuery = "PREFIX prov:<http://www.w3.org/ns/prov#> "
						+ "PREFIX cse:<http://www.citydatafusion.org/ontologies/2014/1/cse#> "
						+ "PREFIX sma:<http://www.citydatafusion.org/ontologies/2014/1/sma#> "
						+ "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#> "
						+ "PREFIX sioc:<http://rdfs.org/sioc/ns#> "
						+ "PREFIX geo:<http://www.w3.org/2003/01/geo/wgs84_pos#> "
						+ "SELECT DISTINCT ?id ?name ?ab ?as ?lat ?long "
						+ "WHERE { "
						+ "{ ?g prov:generatedAtTime ?graphGenTS . "
						+ "FILTER(?graphGenTS = \"" + GeneralUtilities.getXsdDateTime(Long.parseLong(sReq.getWhen())) + "\"^^xsd:dateTime) "
						+ "} "
						+ "{GRAPH ?g { "
						+ "		?stall cse:id ?id ; "
						+ "		cse:name ?name ; "
						+ "		cse:availableBikes ?ab ; "
						+ "		cse:availbaleStalls ?as ; "
						+ "		sma:created_in ?cp ; "
						+ "		geo:location ?l . "
						+ "		?l geo:lat ?lat ; "
						+ "		geo:long ?long . "
						+ "		FILTER(xsd:integer(substr(xsd:string(?cp),59)) IN(" + cellListString + ")) "
						+ "	} "
						+ "} "
						+ "}";
			}


			long startTs = System.currentTimeMillis();
			query = QueryFactory.create(sparqlQuery,Syntax.syntaxSPARQL_11);
			qexec = QueryExecutionFactory.createServiceRequest(Config.getInstance().getBikemiSparqlEndpointURL(), query);

			ResultSet rs = qexec.execSelect();

			long endTs = System.currentTimeMillis();

			logger.debug("Stalls query done, time: {} ms",endTs - startTs);
			Stall stall;

			StallsResponse response = new StallsResponse();
			int totalDocks;
			int availableBikes;
			
//			System.out.println(ResultSetFormatter.asText(rs));

			while (rs.hasNext()) {
				QuerySolution qs = (QuerySolution) rs.next();

				stall = new Stall();

				availableBikes = Integer.parseInt(qs.getLiteral("ab").getLexicalForm());
				stall.setId(qs.getLiteral("id").getLexicalForm());
				stall.setName(qs.getLiteral("name").getLexicalForm());
				stall.setLatitude(Double.parseDouble(qs.getLiteral("lat").getLexicalForm()));
				stall.setLongitude(Double.parseDouble(qs.getLiteral("long").getLexicalForm()));
				stall.setAvailableBikes(availableBikes);
				totalDocks = availableBikes + Integer.parseInt(qs.getLiteral("as").getLexicalForm());
				stall.setDocks(totalDocks);
				if(totalDocks != 0)
					stall.setPercentageOfAvailableBikes((1 - (double) availableBikes / (double) totalDocks));
				else
					stall.setPercentageOfAvailableBikes(0);

				response.addElementToList(stall);
			}

			startTs = System.currentTimeMillis();
			String respString = gson.toJson(response);
			endTs = System.currentTimeMillis();

			logger.debug("Stalls json serialization done, time: {} ms",endTs - startTs);

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
