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
import it.polimi.deib.city_sensing_server.users.utilities.User4Topic;
import it.polimi.deib.city_sensing_server.users.utilities.UsersSALink;
import it.polimi.deib.city_sensing_server.users.utilities.UsersSANode;
import it.polimi.deib.city_sensing_server.utilities.GeneralUtilities;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Set;

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
				uReq.setStart(String.valueOf(Long.parseLong(Config.getInstance().getDefaultStart())));
			}
			if(uReq.getEnd() == null || Long.parseLong(uReq.getEnd()) < 0){
				uReq.setEnd(String.valueOf(Long.parseLong(Config.getInstance().getDefaultEnd())));
			}
			if(uReq.getUsers() != null || uReq.getUsers().size() != 0){
				for(String s : uReq.getUsers()){
					userListString = userListString + "\"" + s + "\"" + "^^xsd:integer,";
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
					+ "SELECT DISTINCT ?user ?userID ?userName ?avatar ?totalCount "
					+ "WHERE { "
					+ "	{ ?g prov:generatedAtTime ?graphGenTS . "
					+ "		FILTER(?graphGenTS >= \"" + GeneralUtilities.getXsdDateTime(Long.parseLong(uReq.getStart())) + "\"^^xsd:dateTime && ?graphGenTS <= \"" + GeneralUtilities.getXsdDateTime(Long.parseLong(uReq.getEnd())) + "\"^^xsd:dateTime) "
					+ "	} "
					+ "{ SELECT ?user (COUNT(?mp) AS ?totalCount) "
					+ "	WHERE { "
					+ "		{GRAPH ?g { "
					+ "				?mp sioc:has_creator ?user ; "
					+ "				sioc:topic ?venue ; "
					+ "				sma:created_in ?cpRes . "
					//					+ "				?user sioc:id ?userID . "
					+ "				FILTER(xsd:integer(substr(xsd:string(?user),61)) IN (" + userListString + ")) "
					//					+ "				FILTER (REGEX(xsd:string(?venue),'venue','i')) "
					+ "				} "
					+ "			} "
					+ "		} GROUP BY ?user "
					+ "} "
					+ "	{GRAPH ?g { "
					+ "		?user sioc:name ?userName ; "
					+ "		sioc:id ?userID . "
					+ "		OPTIONAL { "
					+ "			?user sioc:avatar ?avatar . "
					+ "		} "
					//					+ "		FILTER(xsd:integer(substr(xsd:string(?user),60)) IN (" + userListString + ")) "
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

			String userNode = new String();

			while (rs.hasNext()) {
				QuerySolution qs = (QuerySolution) rs.next();

				node = new UsersSANode();
				node.setId(qs.getLiteral("userID").getLexicalForm());
				userNode = userNode + "\"" + node.getId() + "\"" + "^^xsd:integer,";
				node.setName(qs.getLiteral("userName").getLexicalForm());
				node.setSocialActivity(Long.parseLong(qs.getLiteral("totalCount").getLexicalForm()));
				if(qs.getLiteral("avatar") != null)
					node.setAvatar(qs.getLiteral("avatar").getLexicalForm());
				else
					node.setAvatar("");

				response.addNode(node);
			}

			if(userNode.contains(","))
				userNode = userNode.substring(0, userNode.lastIndexOf(","));

			//			sparqlQuery = "PREFIX prov:<http://www.w3.org/ns/prov#> "
			//					+ "PREFIX cse:<http://www.citydatafusion.org/ontologies/2014/1/cse#> "
			//					+ "PREFIX sma:<http://www.citydatafusion.org/ontologies/2014/1/sma#> "
			//					+ "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#> "
			//					+ "PREFIX sioc:<http://rdfs.org/sioc/ns#> "
			//					+ "PREFIX geo:<http://www.w3.org/2003/01/geo/wgs84_pos#> "
			//					+ "SELECT DISTINCT ?user1 ?user2 ?totalCount "
			//					+ "WHERE { "
			//					+ "	{ SELECT ?user1 ?user2 (COUNT(?mp1) AS ?totalCount) "
			//					+ "	  WHERE { "
			//					+ "			{ ?g prov:generatedAtTime ?graphGenTS . "
			//					+ "	  			FILTER(?graphGenTS >= \"" + GeneralUtilities.getXsdDateTime(Long.parseLong(uReq.getStart())) + "\"^^xsd:dateTime && ?graphGenTS <= \"" + GeneralUtilities.getXsdDateTime(Long.parseLong(uReq.getEnd())) + "\"^^xsd:dateTime) "
			//					+ "			} "
			//					+ "			{GRAPH ?g { "
			//					+ "				?user1 sioc:creator_of ?mp1 . "
			//					+ "				?user2 sioc:creator_of ?mp2 . "
			//					+ "				{ "
			//					+ "				?mp1 sioc:topic ?topic1 . "
			//					+ "				?mp2 sioc:topic ?topic2 . "
			//					+ "				FILTER(xsd:string(?topic1) = xsd:string(?topic2)) "
			//					+ "				} "
			////					+ "				UNION { "
			////					+ "				?mp1 sma:created_in ?cp1 . "
			////					+ "				?mp2 sma:created_in ?cp2 . "
			////					+ "				BIND (xsd:integer(substr(xsd:string(?cp1),59)) AS ?cp1l) . "
			////					+ "				BIND (xsd:integer(substr(xsd:string(?cp2),59)) AS ?cp2l) . "
			////					+ " 			FILTER(?cp1l IN (\"(?cp2l - 1)\"^^xsd:integer,\"(?cp2l + 1)\"^^xsd:integer,\"(?cp2l - 1000)\"^^xsd:integer,\"(?cp2l + 1000)\"^^xsd:integer,\"(?cp2l - 999)\"^^xsd:integer,\"(?cp2l + 999)\"^^xsd:integer,\"(?cp2l - 1001)\"^^xsd:integer,\"(?cp2l + 1001)) || ?cp2l IN ((?cp1l - 1)\"^^xsd:integer,\"(?cp1l + 1)\"^^xsd:integer,\"(?cp1l - 1000)\"^^xsd:integer,\"(?cp1l + 1000)\"^^xsd:integer,\"(?cp1l - 999)\"^^xsd:integer,\"(?cp1l + 999)\"^^xsd:integer,\"(?cp1l - 1001)\"^^xsd:integer,\"(?cp1l + 1001)\"^^xsd:integer)) "
			////					+ "				} . "
			//					+ "				FILTER(xsd:integer(substr(xsd:string(?user1),61)) > xsd:integer(substr(xsd:string(?user2),61)) && xsd:integer(substr(xsd:string(?user1),61)) IN (" + userListString + ") && xsd:integer(substr(xsd:string(?user2),61)) IN (" + userListString + ")) "
			//					+ "				} "
			//					+ "			} "
			//					+ "		} GROUP BY ?user1 ?user2 "
			//					+ "	} "
			//					+ "} "
			//					+ "ORDER BY DESC(?totalCount) "
			//					+ "LIMIT " + uReq.getThreshold();

			sparqlQuery = "PREFIX prov:<http://www.w3.org/ns/prov#> "
					+ "PREFIX cse:<http://www.citydatafusion.org/ontologies/2014/1/cse#> "
					+ "PREFIX sma:<http://www.citydatafusion.org/ontologies/2014/1/sma#> "
					+ "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#> "
					+ "PREFIX sioc:<http://rdfs.org/sioc/ns#> "
					+ "PREFIX geo:<http://www.w3.org/2003/01/geo/wgs84_pos#> "
					+ "SELECT ?user ?topic "
					+ "WHERE { "
					+ "{ SELECT ?user ?topic "
					+ " WHERE { "
					+ "		{ ?g prov:generatedAtTime ?graphGenTS . "
					+ "	  		FILTER(?graphGenTS >= \"" + GeneralUtilities.getXsdDateTime(Long.parseLong(uReq.getStart())) + "\"^^xsd:dateTime && ?graphGenTS <= \"" + GeneralUtilities.getXsdDateTime(Long.parseLong(uReq.getEnd())) + "\"^^xsd:dateTime) "					
					+ "		} "	
					+ "		{GRAPH ?g { "
					+" 			?user sioc:creator_of ?mp . "
					+ "			?mp sioc:topic ?topic . "
					+ "			BIND(xsd:integer(substr(xsd:string(?user),61)) AS ?ul) . " 
					+ "			FILTER(?ul IN (" + userListString + ")) "
					+ "			} "
					+ "		} "
					+ "	} "
					+ "} "
					+ "}";	



			query = QueryFactory.create(sparqlQuery,Syntax.syntaxSPARQL_11);
			qexec = QueryExecutionFactory.createServiceRequest(Config.getInstance().getTweetsSparqlEndpointURL(), query);

			rs = qexec.execSelect();

			//			System.out.println(ResultSetFormatter.asText(rs));

			long endTs = System.currentTimeMillis();

			logger.debug("User Social Activity query done, time: {} ms",endTs - startTs);
			UsersSALink link;
			String user;
			HashMap<String, User4Topic> u4tMap = new HashMap<String, User4Topic>();
			User4Topic u4t;

			while (rs.hasNext()) {
				QuerySolution qs = (QuerySolution) rs.next();

				u4t = new User4Topic();
				user = qs.getResource("user").toString();
				if(u4tMap.containsKey(user)){
					u4t = u4tMap.get(user);
					u4t.addElement(qs.getResource("topic").toString());
					u4tMap.put(user, u4t);
				} else {
					u4t = new User4Topic();
					u4t.setURI(user);
					u4t.addElement(qs.getResource("topic").toString());
					u4tMap.put(user, u4t);
				}
			}

			Set<String> keyList = u4tMap.keySet();
			User4Topic u4tTemp;
			long u4tUriValue = 0;
			long u4tTempUriValue = 0;

			int cont = 0;
			for(String key : keyList){
				u4t = u4tMap.get(key);
				user = u4t.getURI();
				u4tUriValue = Long.parseLong(user.substring(user.lastIndexOf("/") + 1, user.length()));
				for(String key2 : keyList){
					u4tTemp = u4tMap.get(key2);
					user = u4tTemp.getURI();
					u4tTempUriValue = Long.parseLong(user.substring(user.lastIndexOf("/") + 1, user.length()));
					if(u4tUriValue > u4tTempUriValue && !u4t.isAlreadyChecked()) {
						cont = u4t.isIn(u4tTemp.getTopics());
						if(cont > 0){
							link = new UsersSALink();
							link.setSource(String.valueOf(u4tUriValue));
							link.setTarget(String.valueOf(u4tTempUriValue));
							link.setValue((double) cont);
							response.addLink(link);

						}
					}
				}
				u4t.setAlreadyChecked();
				u4tMap.put(key, u4t);
			}

			//			while (rs.hasNext()) {
			//				QuerySolution qs = (QuerySolution) rs.next();
			//
			//				if(qs.contains("user1") && qs.contains("user2")){
			//
			//					link = new UsersSALink();
			//					user = qs.getResource("user1").toString();
			//					link.setSource(user.substring(user.lastIndexOf("/") + 1, user.length()));
			//					user = qs.getResource("user2").toString();
			//					link.setTarget(user.substring(user.lastIndexOf("/") + 1, user.length()));	
			//					link.setValue(Double.parseDouble(qs.getLiteral("totalCount").getLexicalForm()));
			//
			//					response.addLink(link);
			//				}
			//			}

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
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			if(qexec != null)
				qexec.close();
			rep.release();
		}
	}

}
