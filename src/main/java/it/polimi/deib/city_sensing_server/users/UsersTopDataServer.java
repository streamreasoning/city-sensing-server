/*******************************************************************************
 * Copyright 2014 DEIB - Politecnico di Milano
 *    
 * Marco Balduini (marco.balduini@polimi.it)
 * Emanuele Della Valle (emanuele.dellavalle@polimi.it)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package it.polimi.deib.city_sensing_server.users;

import it.polimi.deib.city_sensing_server.configuration.Config;
import it.polimi.deib.city_sensing_server.users.utilities.User;
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


public class UsersTopDataServer extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(UsersTopDataServer.class.getName());

	@SuppressWarnings({ "unchecked", "rawtypes"})
	@Post
	public void dataServer(Representation rep) throws IOException {

		Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().serializeNulls().create();
		Query query = null;
		QueryExecution qexec = null;

		try {

			logger.debug("Users Top request received");
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

			UsersTopRequest uReq = gson.fromJson(reader, UsersTopRequest.class);

			String cellListString = new String();

			boolean allCells = false;

			if(uReq.getStart() == null || Long.parseLong(uReq.getStart()) < 0){
				uReq.setStart(String.valueOf(Long.parseLong(Config.getInstance().getDefaultStart())));
			}
			if(uReq.getEnd() == null || Long.parseLong(uReq.getEnd()) < 0){
				uReq.setEnd(String.valueOf(Long.parseLong(Config.getInstance().getDefaultEnd())));
			}
			if(uReq.getCells() == null || uReq.getCells().size() == 0){
				allCells = true;
			} else {
				for(String s : uReq.getCells()){
					cellListString = cellListString + "\"" + s + "\"" + "^^xsd:long,";
				}
				cellListString = cellListString.substring(0, cellListString.lastIndexOf(","));
			}
			if(uReq.getThreshold() == null){
				uReq.setThreshold(String.valueOf(100));
			}

			String sparqlQuery;

			if(allCells){
				sparqlQuery = "PREFIX prov:<http://www.w3.org/ns/prov#> "
						+ "PREFIX cse:<http://www.citydatafusion.org/ontologies/2014/1/cse#> "
						+ "PREFIX sma:<http://www.citydatafusion.org/ontologies/2014/1/sma#> "
						+ "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#> "
						+ "PREFIX sioc:<http://rdfs.org/sioc/ns#> "
						+ "PREFIX geo:<http://www.w3.org/2003/01/geo/wgs84_pos#> "
						+ "SELECT DISTINCT ?user ?userID ?userName ?avatar ?totalCount "
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
						+ "				?cpRes cse:city_pixel_ID ?cpId . "
//						+ "				FILTER(?cpId > 0 && ?cpId < 10000) "
						+ "				FILTER (REGEX(xsd:string(?venue),'venue','i')) "
						+ "				} "
						+ "			} "
						+ "		} GROUP BY ?user  "
						+ "} "
						+ "	{GRAPH ?g { "
						+ "		?user sioc:id ?userID ; "
						+ "		sioc:name ?userName . "
						+ "		OPTIONAL { "
						+ "			?user sioc:avatar ?avatar . "
						+ "		} "
						+ "	} "
						+ "} "
						+ "}"
						+ "ORDER BY DESC(?totalCount) "
						+ "LIMIT " + uReq.getThreshold() + "";

			} else{
				sparqlQuery = "PREFIX prov:<http://www.w3.org/ns/prov#> "
						+ "PREFIX cse:<http://www.citydatafusion.org/ontologies/2014/1/cse#> "
						+ "PREFIX sma:<http://www.citydatafusion.org/ontologies/2014/1/sma#> "
						+ "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#> "
						+ "PREFIX sioc:<http://rdfs.org/sioc/ns#> "
						+ "PREFIX geo:<http://www.w3.org/2003/01/geo/wgs84_pos#> "
						+ "SELECT DISTINCT ?user ?userID ?userName ?avatar ?totalCount "
						+ "WHERE { "
						+ "{ ?g prov:generatedAtTime ?graphGenTS . "
						+ "FILTER(?graphGenTS >= \"" + GeneralUtilities.getXsdDateTime(Long.parseLong(uReq.getStart())) + "\"^^xsd:dateTime && ?graphGenTS <= \"" + GeneralUtilities.getXsdDateTime(Long.parseLong(uReq.getEnd())) + "\"^^xsd:dateTime) "
						+ "} "
						+ "{ SELECT ?user (COUNT(?mp) AS ?totalCount) ?cpRes "
						+ "	WHERE { "
						+ "			{GRAPH ?g { "
						+ "				?mp sioc:has_creator ?user ; "
						+ "				sioc:topic ?venue ; "
						+ "				sma:created_in ?cpRes . "
//						+ "				?cpRes cse:city_pixel_ID ?cpId . "
						+ " 			FILTER(xsd:long(substr(xsd:string(?cpRes),59)) IN (" + cellListString + ")) "
						+ "				FILTER (REGEX(xsd:string(?venue),'venue','i')) "
						+ "				} "
						+ "			} "
						+ "		} GROUP BY ?user ?cpRes "
						+ "} "
						+ "	{GRAPH ?g { "
						+ "		?user sioc:id ?userID ; "
						+ "		sioc:name ?userName . "
						+ "		OPTIONAL { "
						+ "			?user sioc:avatar ?avatar . "
						+ "		} "
						+ "	} "
						+ "} "
						+ "}"
						+ "ORDER BY DESC(?totalCount) "
						+ "LIMIT " + uReq.getThreshold() + "";
			}

			long startTs = System.currentTimeMillis();
			query = QueryFactory.create(sparqlQuery,Syntax.syntaxSPARQL_11);
			qexec = QueryExecutionFactory.createServiceRequest(Config.getInstance().getTweetsSparqlEndpointURL(), query);

			ResultSet rs = qexec.execSelect();
			
			long endTs = System.currentTimeMillis();

			logger.debug("User top query done, time: {} ms",endTs - startTs);
			User user;

			UsersTopResponse response = new UsersTopResponse();

			while (rs.hasNext()) {
				QuerySolution qs = (QuerySolution) rs.next();

				user = new User();
				user.setId(qs.getLiteral("userID").getLexicalForm());
				user.setName(qs.getLiteral("userName").getLexicalForm());
				if(qs.getLiteral("avatar") != null)
					user.setImageUrl(qs.getLiteral("avatar").getLexicalForm());
				else
					user.setImageUrl("");
				user.setSocialActivity(Long.parseLong(qs.getLiteral("totalCount").getLexicalForm()));

				response.addElementToList(user);
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
