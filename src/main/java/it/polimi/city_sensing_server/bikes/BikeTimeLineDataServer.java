package it.polimi.city_sensing_server.bikes;

import it.polimi.city_sensing_server.bikes.utilities.BikeStep;
import it.polimi.deib.city_sensing_server.configuration.Config;
import it.polimi.deib.city_sensing_server.utilities.GeneralUtilities;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.bind.DatatypeConverter;
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


public class BikeTimeLineDataServer extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(BikeTimeLineDataServer.class.getName());

	@SuppressWarnings({ "unchecked", "rawtypes"})
	@Post
	public void dataServer(Representation rep) throws IOException {

		Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().serializeNulls().create();
		Query query = null;
		QueryExecution qexec = null;

		try {

			logger.debug("Bikes Time Line request received");
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

			BikeTimeLineRequest bReq = gson.fromJson(reader, BikeTimeLineRequest.class);

			String cellListString = new String();

			boolean allCells = false;

			if(bReq.getStart() == null || Long.parseLong(bReq.getStart()) < 0){
				bReq.setStart(String.valueOf(Long.parseLong(Config.getInstance().getDefaultStart())));
			}
			if(bReq.getEnd() == null || Long.parseLong(bReq.getEnd()) < 0){
				bReq.setEnd(String.valueOf(Long.parseLong(Config.getInstance().getDefaultEnd())));
			}
			if(bReq.getCells() == null || bReq.getCells().size() == 0){
				allCells = true;
			} else {
				for(String s : bReq.getCells()){
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
						+ "SELECT ?graphGenTS (SUM(?ab) AS ?aBikes) (SUM(?as) AS ?aStall) "
						+ "WHERE { "
						+ "{ ?g prov:generatedAtTime ?graphGenTS . "
						+ "FILTER(?graphGenTS >= \"" + GeneralUtilities.getXsdDateTime(Long.parseLong(bReq.getStart())) + "\"^^xsd:dateTime && ?graphGenTS <= \"" + GeneralUtilities.getXsdDateTime(Long.parseLong(bReq.getEnd())) + "\"^^xsd:dateTime) "
						+ "{GRAPH ?g { "
						+ "		?stall cse:availableBikes ?ab ; "
						+ "		cse:availbaleStalls ?as ; "
						+ "		sma:created_in ?cpRes . "
						+ "		?cpRes cse:city_pixel_ID ?cpId . "
						+ "		FILTER(?cpId > 0 && ?cpId < 10000) "
						+ "		} "
						+ "	} "
						+ "} "
						+ "} "
						+ "GROUP BY ?graphGenTS ";

			} else{
				sparqlQuery = "PREFIX prov:<http://www.w3.org/ns/prov#> "
						+ "PREFIX cse:<http://www.citydatafusion.org/ontologies/2014/1/cse#> "
						+ "PREFIX sma:<http://www.citydatafusion.org/ontologies/2014/1/sma#> "
						+ "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#> "
						+ "PREFIX sioc:<http://rdfs.org/sioc/ns#> "
						+ "PREFIX geo:<http://www.w3.org/2003/01/geo/wgs84_pos#> "
						+ "SELECT ?graphGenTS (SUM(?ab) AS ?aBikes) (SUM(?as) AS ?aStall) "
						+ "WHERE { "
						+ "{ ?g prov:generatedAtTime ?graphGenTS . "
						+ "FILTER(?graphGenTS >= \"" + GeneralUtilities.getXsdDateTime(Long.parseLong(bReq.getStart())) + "\"^^xsd:dateTime && ?graphGenTS <= \"" + GeneralUtilities.getXsdDateTime(Long.parseLong(bReq.getEnd())) + "\"^^xsd:dateTime) "
						+ "{GRAPH ?g { "
						+ "		?stall cse:availableBikes ?ab ; "
						+ "		cse:availbaleStalls ?as ; "
						+ "		sma:created_in ?cpRes . "
						+ "		?cpRes cse:city_pixel_ID ?cpId . "
						+ "		FILTER(?cpId IN(" + cellListString + ")) "
						+ "		} "
						+ "	} "
						+ "} "
						+ "} "
						+ "GROUP BY ?graphGenTS ";

			}

			long startTs = System.currentTimeMillis();
			query = QueryFactory.create(sparqlQuery,Syntax.syntaxSPARQL_11);
			qexec = QueryExecutionFactory.createServiceRequest(Config.getInstance().getBikemiSparqlEndpointURL(), query);

			ResultSet rs = qexec.execSelect();

			long endTs = System.currentTimeMillis();

			logger.debug("Bike Time Line query done, time: {} ms",endTs - startTs);

			BikeStep bikeStep;
			BikeTimeLineResponse response = new BikeTimeLineResponse();

			long tsInterval = 900000;

			//			System.out.println(ResultSetFormatter.asText(rs));

			while (rs.hasNext()) {
				QuerySolution qs = (QuerySolution) rs.next();

				if(qs.contains("graphGenTS")){

					bikeStep = new BikeStep();

					bikeStep.setStart(DatatypeConverter.parseDateTime(qs.getLiteral("graphGenTS").getLexicalForm()).getTimeInMillis());
					bikeStep.setEnd(DatatypeConverter.parseDateTime(qs.getLiteral("graphGenTS").getLexicalForm()).getTimeInMillis() + tsInterval);
					bikeStep.setAvailableBikes(Long.parseLong(qs.getLiteral("aBikes").getLexicalForm()));
					bikeStep.setInUseBikes(Long.parseLong(qs.getLiteral("aStall").getLexicalForm()));
					bikeStep.setDocks(Long.parseLong(qs.getLiteral("aStall").getLexicalForm()) + Long.parseLong(qs.getLiteral("aBikes").getLexicalForm()));

					response.addElementToList(bikeStep);
				}
			}

			startTs = System.currentTimeMillis();
			String respString = gson.toJson(response);
			endTs = System.currentTimeMillis();

			logger.debug("Bike Time Line json serialization done, time: {} ms",endTs - startTs);

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