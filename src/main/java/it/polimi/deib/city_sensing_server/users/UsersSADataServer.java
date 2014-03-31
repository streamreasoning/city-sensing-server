package it.polimi.deib.city_sensing_server.users;

import it.polimi.deib.city_sensing_server.configuration.Config;
import it.polimi.deib.city_sensing_server.users.utilities.UsersSALink;
import it.polimi.deib.city_sensing_server.users.utilities.UsersSANode;
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


public class UsersSADataServer extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(UsersSADataServer.class.getName());
	private long actualizationInterval = 30495300000L;

	@SuppressWarnings({ "unchecked", "rawtypes"})
	@Post
	public void dataServer(Representation rep) throws IOException {

		Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().serializeNulls().create();
		Query query = null;
		QueryExecution qexec = null;

		try {

			logger.debug("Users Social Activity request received");
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

			UsersSARequest uReq = gson.fromJson(reader, UsersSARequest.class);

			String userListString = new String();

			if(uReq.getStart() == null || Long.parseLong(uReq.getStart()) < 0){
				uReq.setStart(String.valueOf(Long.parseLong(Config.getInstance().getDefaultStart()) + actualizationInterval));
			}
			if(uReq.getEnd() == null || Long.parseLong(uReq.getEnd()) < 0){
				uReq.setEnd(String.valueOf(Long.parseLong(Config.getInstance().getDefaultEnd()) + actualizationInterval));
			}
			if(uReq.getUsers() != null || uReq.getUsers().size() != 0){
				for(String s : uReq.getUsers()){
					userListString = userListString + "\"" + s + "\"" + ",";
				}
				userListString = userListString.substring(0, userListString.lastIndexOf(","));
			}
			if(uReq.getThreshold() == null){
				uReq.setThreshold(String.valueOf(10));
			}

			String sparqlQuery = "PREFIX prov:<http://www.w3.org/ns/prov#> "
					+ "PREFIX cse:<http://www.citydatafusion.org/ontologies/2014/1/cse#> "
					+ "PREFIX sma:<http://www.citydatafusion.org/ontologies/2014/1/sma#> "
					+ "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#> "
					+ "PREFIX sioc:<http://rdfs.org/sioc/ns#> "
					+ "PREFIX geo:<http://www.w3.org/2003/01/geo/wgs84_pos#> "
					+ "SELECT DISTINCT ?user ?userID ?userName ?totalCount "
					+ "WHERE { "
					+ "{ ?g prov:generatedAtTime ?graphGenTS . "
					+ "FILTER(?graphGenTS >= \"" + GeneralUtilities.getXsdDateTime(Long.parseLong(uReq.getStart())) + "\"^^xsd:dateTime && ?graphGenTS <= \"" + GeneralUtilities.getXsdDateTime(Long.parseLong(uReq.getEnd())) + "\"^^xsd:dateTime) "
					+ "} "
					+ "{ SELECT ?user (COUNT(?mp) AS ?totalCount) "
					+ "	WHERE { "
					+ "			{GRAPH ?g { "
					+ "				?mp sioc:has_creator ?user ; "
					+ "				sioc:topic ?venue ; "
					+ "				sma:created_in ?cpRes . "
					+ "				?user sioc:id ?userID . "
					+ "				FILTER(xsd:string(?userID) IN (" + userListString + ")) "
					+ "				FILTER (REGEX(xsd:string(?venue),'venue','i')) "
					+ "				} "
					+ "			} "
					+ "		} GROUP BY ?user  "
					+ "} "
					+ "	{GRAPH ?g { "
					+ "		?user sioc:name ?userName ; "
					+ "		sioc:id ?userID . "
					+ "	} "
					+ "} "
					+ "}"
					+ "ORDER BY DESC(?totalCount) "
					+ "LIMIT " + uReq.getThreshold();



			long startTs = System.currentTimeMillis();
			query = QueryFactory.create(sparqlQuery,Syntax.syntaxSPARQL_11);
			qexec = QueryExecutionFactory.createServiceRequest(Config.getInstance().getTweetsSparqlEndpointURL(), query);

			ResultSet rs = qexec.execSelect();
			UsersSANode node;

			UsersSAResponse response = new UsersSAResponse();

			while (rs.hasNext()) {
				QuerySolution qs = (QuerySolution) rs.next();

				node = new UsersSANode();
				node.setId(qs.getLiteral("userID").getLexicalForm());
				node.setName(qs.getLiteral("userName").getLexicalForm());
				node.setSocialActivity(Long.parseLong(qs.getLiteral("totalCount").getLexicalForm()));

				response.addNode(node);
			}

			sparqlQuery = "PREFIX prov:<http://www.w3.org/ns/prov#> "
					+ "PREFIX cse:<http://www.citydatafusion.org/ontologies/2014/1/cse#> "
					+ "PREFIX sma:<http://www.citydatafusion.org/ontologies/2014/1/sma#> "
					+ "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#> "
					+ "PREFIX sioc:<http://rdfs.org/sioc/ns#> "
					+ "PREFIX geo:<http://www.w3.org/2003/01/geo/wgs84_pos#> "
					+ "SELECT DISTINCT ?userID1 ?userID2 ?totalCount "
					+ "WHERE { "
					+ "	{ ?g prov:generatedAtTime ?graphGenTS . "
					+ "	  FILTER(?graphGenTS >= \"" + GeneralUtilities.getXsdDateTime(Long.parseLong(uReq.getStart())) + "\"^^xsd:dateTime && ?graphGenTS <= \"" + GeneralUtilities.getXsdDateTime(Long.parseLong(uReq.getEnd())) + "\"^^xsd:dateTime) "
					+ "	} "
					+ "	{ SELECT ?userID1 ?userID2 (COUNT(?topic) AS ?totalCount) "
					+ "	  WHERE { "
					+ "			{GRAPH ?g { "
					+ "				?user1 sioc:creator_of ?mp1 . "
					+ "				?user2 sioc:creator_of ?mp2 . "
					+ "				?mp1 sioc:topic ?topic . "
					+ "				?mp2 sioc:topic ?topic . "
					+ "				?user1 sioc:id ?userID1 . "
					+ "				?user2 sioc:id ?userID2 . "
					+ "				FILTER(xsd:long(?userID1) > xsd:long(?userID2) && xsd:string(?userID1) IN (" + userListString + ") && xsd:string(?userID2) IN (" + userListString + ")) "
					+ "				} "
					+ "			} "
					+ "		} GROUP BY ?userID1 ?userID2 "
					+ "	} "
					+ "} "
					+ "ORDER BY DESC(?totalCount) "
					+ "LIMIT " + uReq.getThreshold();



			query = QueryFactory.create(sparqlQuery,Syntax.syntaxSPARQL_11);
			qexec = QueryExecutionFactory.createServiceRequest(Config.getInstance().getTweetsSparqlEndpointURL(), query);

			rs = qexec.execSelect();

			long endTs = System.currentTimeMillis();

			logger.debug("User Social Activity query done, time: {} ms",endTs - startTs);
			UsersSALink link;

			while (rs.hasNext()) {
				QuerySolution qs = (QuerySolution) rs.next();

				if(qs.contains("userID1")){

					link = new UsersSALink();
					link.setSource(qs.getLiteral("userID1").getLexicalForm());
					link.setTarget(qs.getLiteral("userID2").getLexicalForm());
					link.setValue(Double.parseDouble(qs.getLiteral("totalCount").getLexicalForm()));

					response.addLink(link);
				}
			}

			startTs = System.currentTimeMillis();
			String respString = gson.toJson(response);
			endTs = System.currentTimeMillis();

			logger.debug("User Social Activity json serialization done, time: {} ms",endTs - startTs);

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
