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
package it.polimi.deib.city_sensing_server.utilities;

import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.query.DatasetAccessor;
import com.hp.hpl.jena.query.DatasetAccessorFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.impl.PropertyImpl;
import com.hp.hpl.jena.rdf.model.impl.ResourceImpl;
import com.hp.hpl.jena.vocabulary.RDF;

public class RDFRemoteDsMediator {

	private DatasetAccessor da;
	private String provPrefix = "http://www.w3.org/ns/prov#";
	private String baseURI;
	private String streamName;

	private GregorianCalendar gc;
	private GregorianCalendar startGc;
	private GregorianCalendar endGc;
	private XMLGregorianCalendar xmlCalendar;
	private XMLGregorianCalendar startXmlCalendar;
	private XMLGregorianCalendar endXmlCalendar;

	private String datasetQueryURL;

	public RDFRemoteDsMediator(String datasetDataURL, String datasetQueryURL, String baseURI, String streamname) {
		super();

		da = DatasetAccessorFactory.createHTTP(datasetDataURL);
		this.baseURI = baseURI;
		this.streamName = streamname;
		this.datasetQueryURL = datasetQueryURL;

	}

	public RDFRemoteDsMediator(String datasetDataURL, String baseURI, String streamname) {
		super();

		da = DatasetAccessorFactory.createHTTP(datasetDataURL);
		this.baseURI = baseURI;
		this.streamName = streamname;
		this.datasetQueryURL = "";

	}

//	public RDFRemoteDsMediator(String datasetDataURL) {
//		super();
//
//		da = DatasetAccessorFactory.createHTTP(datasetDataURL);
//		this.baseURI = "";
//		this.streamName = "";
//		this.datasetQueryURL = "";
//
//	}

	public Model createGraphMetadata(String graphName, long timestamp) throws DatatypeConfigurationException{

		Model tempMetadataGraph = ModelFactory.createDefaultModel();

		tempMetadataGraph.add(new ResourceImpl(graphName), RDF.type, new ResourceImpl(provPrefix + "Entity"));
		tempMetadataGraph.add(new ResourceImpl(graphName), new PropertyImpl(provPrefix + "wasAttributedTo"), new ResourceImpl(baseURI + streamName));

		gc = new GregorianCalendar();
		gc.setTimeInMillis(timestamp);
		xmlCalendar = DatatypeFactory.newInstance().newXMLGregorianCalendar(gc);

		tempMetadataGraph.add(new ResourceImpl(graphName), new PropertyImpl(provPrefix + "generatedAtTime"), tempMetadataGraph.createTypedLiteral(xmlCalendar.toXMLFormat(), XSDDatatype.XSDdateTime));

		return tempMetadataGraph;

	}

	public void putNewGraph(String graphName, Model model, long timestamp) throws DatatypeConfigurationException{
		da.putModel(graphName, model);
		da.add(createGraphMetadata(graphName, timestamp));
	}

	public List<String> getRecentGraphNames(long timestamp) throws DatatypeConfigurationException {

		gc = new GregorianCalendar();
		gc.setTimeInMillis(timestamp);
		xmlCalendar = DatatypeFactory.newInstance().newXMLGregorianCalendar(gc);

		String query = "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> "
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>  "
				+ "PREFIX prov: <http://www.w3.org/ns/prov#> "
				+ "SELECT ?g " 
				+ "WHERE { " 
				+ "?g a prov:Entity ; "
				+ "prov:wasAttributedTo " + baseURI + streamName + " ; "
				+ "prov:generatedAtTime ?t . " 
				+ "FILTER (?t >= \""+ xmlCalendar.toXMLFormat() + "\"^^xsd:dateTime) "
				+ "}";

		List<String> l = new LinkedList<String>();

		Query q = QueryFactory.create(query, Syntax.syntaxSPARQL_11);
		QueryExecution qexec = QueryExecutionFactory.sparqlService(datasetQueryURL, q);
		ResultSet r = qexec.execSelect();
		for (; r.hasNext();) {
			QuerySolution soln = r.nextSolution();
			l.add(soln.get("g").toString());
		}

		return l;

	}

	public List<String> getRecentGraphNames(long startTs,long endTs) throws DatatypeConfigurationException {

		startGc = new GregorianCalendar();
		gc.setTimeInMillis(startTs);
		endGc = new GregorianCalendar();
		gc.setTimeInMillis(endTs);
		
		startXmlCalendar = DatatypeFactory.newInstance().newXMLGregorianCalendar(startGc);
		endXmlCalendar = DatatypeFactory.newInstance().newXMLGregorianCalendar(endGc);

		String query = "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> "
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>  "
				+ "PREFIX prov: <http://www.w3.org/ns/prov#> "
				+ "SELECT ?g " 
				+ "WHERE { " 
				+ "?g a prov:Entity ; "
				+ "prov:wasAttributedTo " + baseURI + streamName + " ; "
				+ "prov:generatedAtTime ?t . " 
				+ "FILTER (?t >= \""+ startXmlCalendar.toXMLFormat() + "\"^^xsd:dateTime && ?t <= \""+ endXmlCalendar.toXMLFormat() + "\"^^xsd:dateTime) "
				+ "}";

		List<String> l = new LinkedList<String>();

		Query q = QueryFactory.create(query, Syntax.syntaxSPARQL_11);
		QueryExecution qexec = QueryExecutionFactory.sparqlService(datasetQueryURL, q);
		ResultSet r = qexec.execSelect();
		for (; r.hasNext();) {
			QuerySolution soln = r.nextSolution();
			l.add(soln.get("g").toString());
		}

		return l;

	}

	public Model getGraph(String graphName) {
		return da.getModel(graphName);
	}

	public void addTripleToDefaultGraph(Model model) {
		da.add(model);
	}



}
