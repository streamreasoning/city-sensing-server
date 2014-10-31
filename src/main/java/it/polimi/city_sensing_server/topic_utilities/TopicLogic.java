package it.polimi.city_sensing_server.topic_utilities;

import it.polimi.deib.city_sensing_server.configuration.Config;

import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.javaml.clustering.Clusterer;
import net.sf.javaml.clustering.KMeans;
import net.sf.javaml.core.Dataset;
import net.sf.javaml.core.DefaultDataset;
import net.sf.javaml.core.DenseInstance;
import net.sf.javaml.core.Instance;
import net.sf.javaml.distance.CosineSimilarity;


public class TopicLogic extends Logic{
	
	private ClusterList clusterList;
	private int num_cluster;
	private double cooc_threshold;
	private Date startDate;
	private Date endDate;
	private int NUM_ITER;
	
	static CharsetEncoder asciiEncoder = Charset.forName("ISO-8859-1").newEncoder(); 
	
	private static Logger logger = LoggerFactory.getLogger(TopicLogic.class);
	
	public static boolean isPureAscii(String v) {
		return asciiEncoder.canEncode(v);
	}
	
	public TopicLogic(ClusterList clusterList) throws ClassNotFoundException, SQLException {
		super();
		try {
			NUM_ITER = Integer.parseInt(Config.getInstance().getTopicMaxIterKMeans());
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		this.clusterList = clusterList;
	}
	
	public void setParameters(int k, double threshold){
		num_cluster = k;
		cooc_threshold = (double)(threshold / 100);
	}
	
	public void buildClusters(String startDate, String endDate, int dayMode, int timeMode, int d1, int d2) throws SQLException, ParseException {		
		clusterList.removeAll();
		ArrayList<Cluster> clusterTable = new ArrayList<Cluster>();
		String query ="select h.tag from hashtag h join occorrenza o on h.pk_tag = o.tag join tweet t on o.tweet = t.pk_tweet where t.day >= ? and t.day <= ? group by h.tag order by count(*) desc limit 100";
		
		Dataset d = new DefaultDataset();
		PreparedStatement pstat = conn.prepareStatement(query);
		pstat.setDate(1, parseUtilDate(this.startDate = format.parse(startDate)));
		pstat.setDate(2, parseUtilDate(this.endDate = format.parse(endDate)));
		ResultSet rs = pstat.executeQuery();
		while(rs.next()) {
			if(isPureAscii(rs.getString(1))){
				buildAggregatePattern(rs.getString(1), format.parse(startDate), format.parse(endDate), dayMode, timeMode, d1, d2);
				double[] v = buildAggregateVector();
				Instance inst = new DenseInstance(v, rs.getString(1));
				d.add(inst);
			}
		}
		CosineSimilarity c = new CosineSimilarity();
		Clusterer km = new KMeans(num_cluster, NUM_ITER, c);
		Dataset[] clusters = km.cluster(d);
		for(int j=0; j<clusters.length; j++){
			for(int k=0; k<clusters[j].size(); k++){
				Instance current = clusters[j].get(k);
				if(k==0){
					clusterTable.add(new Cluster(current.classValue().toString(), rebuildPattern(current.values(), d2)));
				} else {
					clusterTable.get(clusterTable.size()-1).setTag(current.classValue().toString(), rebuildPattern(current.values(), d2));
				}
			}
		}
		for(int i=0; i<clusterTable.size(); i++){
			CooccourenceMatrix matrix = new CooccourenceMatrix(clusterTable.get(i).getDim());
			buildMatrix(matrix, clusterTable.get(i));
			Breaker breaker = new Breaker(matrix, clusterTable.get(i), cooc_threshold);
			ArrayList<Cluster> broken = breaker.clustering();
			for(int j=0; j<broken.size(); j++){
				setBestDistances(broken.get(j), matrix);
				for(int k=0; k<broken.get(j).getDim(); k++){
					if(k == 0){
						clusterList.startNew(broken.get(j).getTag(k), broken.get(j).getPattern(k), broken.get(j).getDistance(k));
					} else {
						clusterList.update(clusterList.getNumClusters(), broken.get(j).getTag(k), broken.get(j).getPattern(k), broken.get(j).getDistance(k));
					}
				}
			}
		}
		for(int i=0; i<clusterList.getNumClusters()+1; i++){
			clusterList.getClusterClass(i).setPatternDimension(dayDifference(format.parse(startDate), format.parse(endDate)) + 1, 24);
			clusterList.getClusterClass(i).setPattern(buildCumulativePattern(i, clusterList.getClusterClass(i).getPatternDimension(), format.parse(startDate), format.parse(endDate), d2, timeMode));
		}
		clusterList.orderByRelevance();
		for(int i=0; i<clusterList.getNumClusters() +1; i++){
			clusterList.getClusterClass(i).orderClusterElement();
		}
	}
	
	public double[][] buildCumulativePattern(int clusterCode, int length, Date startDate, Date endDate, int d2, int timeMode) throws SQLException {
		double[] pattern = new double[length];
		for(int i=0; i<length; i++){
			pattern[i] = 0.0;
		}
		String query = "select T.day, T.hour, count(distinct(T.tweet)) from occorrenza O join tweet T on O.tweet = T.pk_tweet where T.day >= ? and T.day <= ? and (O.tag = ?";
		for(int i=1; i<clusterList.getClusterClass(clusterCode).getDim(); i++){
			query += " or O.tag = ?";
		}
		query += ") group by T.day, T.hour";
		PreparedStatement pstat = conn.prepareStatement(query);
		pstat.setDate(1, parseUtilDate(startDate));
		pstat.setDate(2, parseUtilDate(endDate));
		for(int i=0; i<clusterList.getClusterClass(clusterCode).getDim(); i++){
			pstat.setInt(3+i, getHashtagCode(clusterList.getClusterClass(clusterCode).getTag(i)));
		}
		ResultSet rs = pstat.executeQuery(); 
		while(rs.next()){
			pattern[getDateIndex(rs.getDate(1)) * 24 + rs.getInt(2)] = (double)(rs.getDouble(3));
		}
		double[][] tempPattern = getPattern(pattern, 24);
		return sumHours(tempPattern, timeMode, d2);
	}
	
	
	
/**	private void setDistances(Cluster c, CooccourenceMatrix m){
		ClusterElement center = c.getClusterCenter();
		for(int i=0; i<c.getDim(); i++){
			if(c.getTag(i).equals(center.getTag())){
				c.setDistance(i, 0.0);
			}
			else {
				double value = m.getMatrixValue(center.getTag(), c.getTag(i));
				if(value < 1){
					c.setDistance(i, 1.0 - value);
				} else {
					c.setDistance(i, 0.01);
				}
			}
		}
	}
	*/
	/**
	 * setDistance alternativo: sceglie come centro del cluster l'elemento per il quale 
	 * la media delle distanze con gli altri elementi è più piccola
	 */
	
	private void setBestDistances(Cluster c, CooccourenceMatrix m){
		String center = c.getTag(0);
		double meanDistances = 0.0;
		double[] distances = new double[c.getDim()];
		for(int i=0; i<c.getDim(); i++){
			if(c.getTag(i).equals(center)){
				distances[i] = 0.0;
			}
			else {
				double value = m.getMatrixValue(center, c.getTag(i));
				if(value < 1){
					distances[i] = 1.0 - value;
				} else {
					distances[i] = 0.01;
				}
			}
		}
		meanDistances = MathUtils.mean(distances);
		for(int i=1; i<c.getDim(); i++){
			String currentCenter = c.getTag(i);
			double[] currentDistances = new double[c.getDim()];
			for(int j=0; j<c.getDim(); j++){
				if(c.getTag(j).equals(currentCenter)){
					currentDistances[j] = 0.0;
				}
				else {
					double value = m.getMatrixValue(currentCenter, c.getTag(i));
					if(value < 1){
						currentDistances[j] = 1.0 - value;
					} else {
						currentDistances[j] = 0.01;
					}
				}
			}
			if(MathUtils.mean(currentDistances) < meanDistances){
				center = currentCenter;
				distances = currentDistances;
				meanDistances = MathUtils.mean(currentDistances);
			}
		}
		for(int i=0; i<c.getDim(); i++){
			c.setDistance(i, distances[i]);
		}
	}
	
	private double[] buildAggregateVector(){
		double[] v = new double[aggregatePattern.length * aggregatePattern[0].length];
		for(int i=0; i<aggregatePattern.length; i++){
			for(int j=0; j<aggregatePattern[0].length; j++){
				v[(i * aggregatePattern[0].length + j)] = aggregatePattern[i][j];
			}
		}
		return v;
	}
	

	private void buildMatrix(CooccourenceMatrix m, Cluster c) throws SQLException {
		String totalQuery = "select sum(occorrenze) from hitforday where tag = ? and day >=? and day <= ?";
		int[] totalValue = new int [c.getDim()];
		for(int i=0; i<c.getDim(); i++){
			m.setLabel(c.getTag(i), i);
			PreparedStatement totStat = conn.prepareStatement(totalQuery);
			totStat.setString(1, c.getTag(i));
			totStat.setDate(2, parseUtilDate(this.startDate));
			totStat.setDate(3, parseUtilDate(this.endDate));
			ResultSet rs = totStat.executeQuery();
			rs.next();
			totalValue[i] = rs.getInt(1);
		}
		String coocQuery = "select H1.tag, count (T1.pk_tweet) from (select T.pk_tweet from tweet T join occorrenza O on T.pk_tweet = O.tweet join hashtag H on O.tag = H.pk_tag where H.tag = ? and T.day >=? and T.day <=?) T1 join occorrenza O1 on T1.pk_tweet = O1.tweet join hashtag H1 on O1.tag = H1.pk_tag where H1.tag != ? group by H1.tag order by count(T1.pk_tweet)";
		PreparedStatement pstatCooc = null;
		pstatCooc = conn.prepareStatement(coocQuery);
		for(int i=0; i<c.getDim(); i++){
			pstatCooc.setString(1, m.getLabel(i));
			pstatCooc.setDate(2, parseUtilDate(this.startDate));
			pstatCooc.setDate(3, parseUtilDate(this.endDate));
			pstatCooc.setString(4, m.getLabel(i));
			ResultSet rs = pstatCooc.executeQuery();
			int index = -1;
			while(rs.next()){
				if((index = getIndexValue(c, rs.getString(1)))!= -1){
					double total = -1;
					if(totalValue[i]>totalValue[index]){
						total = totalValue[i];
					}
					else{
						total = totalValue[index];
					}		
					double value = (double)(rs.getInt(2)/total);
					if(Double.isNaN(value)){
						System.out.println("Ho trovato un NaN per l'incrocio " + m.getLabel(i) + " - " + m.getLabel(index) + ": Total Value " + m.getLabel(i) + " = " + totalValue[i] + ", " + m.getLabel(index) + " = " + totalValue[index] + "Selected total value: " + total + ", Cooc value: " + rs.getInt(2));
					}
					m.setMatrixValue(i, index, value);
				}
			}
		}
	}
	
	public void buildMatrix(CooccourenceMatrix m, String[] c) throws SQLException {
		String totalQuery = "select sum(occorrenze) from hitforday where tag = ? and day >=? and day <= ?";
		int[] totalValue = new int [c.length];
		for(int i=0; i<c.length; i++){
			m.setLabel(c[i], i);
			PreparedStatement totStat = conn.prepareStatement(totalQuery);
			totStat.setString(1, c[i]);
			totStat.setDate(2, parseUtilDate(this.startDate));
			totStat.setDate(3, parseUtilDate(this.endDate));
			ResultSet rs = totStat.executeQuery();
			rs.next();
			totalValue[i] = rs.getInt(1);
		}
		String coocQuery = "select H1.tag, count (T1.pk_tweet) from (select T.pk_tweet from tweet T join occorrenza O on T.pk_tweet = O.tweet join hashtag H on O.tag = H.pk_tag where H.tag = ? and T.day >=? and T.day <=?) T1 join occorrenza O1 on T1.pk_tweet = O1.tweet join hashtag H1 on O1.tag = H1.pk_tag where H1.tag != ? group by H1.tag order by count(T1.pk_tweet)";
		PreparedStatement pstatCooc = null;
		pstatCooc = conn.prepareStatement(coocQuery);
		for(int i=0; i<c.length; i++){
			pstatCooc.setString(1, m.getLabel(i));
			pstatCooc.setDate(2, parseUtilDate(this.startDate));
			pstatCooc.setDate(3, parseUtilDate(this.endDate));
			pstatCooc.setString(4, m.getLabel(i));
			ResultSet rs = pstatCooc.executeQuery();
			int index = -1;
			while(rs.next()){
				if((index = getIndexValue(c, rs.getString(1)))!= -1){
					double total = -1;
					if(totalValue[i]>totalValue[index]){
						total = totalValue[i];
					}
					else{
						total = totalValue[index];
					}		
					double value = (double)(rs.getInt(2)/total);
					if(Double.isNaN(value)){
						System.out.println("Ho trovato un NaN per l'incrocio " + m.getLabel(i) + " - " + m.getLabel(index) + ": Total Value " + m.getLabel(i) + " = " + totalValue[i] + ", " + m.getLabel(index) + " = " + totalValue[index] + "Selected total value: " + total + ", Cooc value: " + rs.getInt(2));
					}
					m.setMatrixValue(i, index, value);
				}
			}
		}
	}
	
	private double[][] getPattern(double[] vector, int s){
		double pattern[][] = new double[vector.length / s][s];
		for(int i=0; i<vector.length/s; i++){
			for(int j=0; j<s; j++){
				pattern[i][j] = vector[i*s+j];
			}
		}
		return pattern;
	}
	
	private double[][] rebuildPattern(Collection<Double> vector, int s){
		double[] collection = new double[vector.size()];
		Iterator<Double> iter = vector.iterator();
		int index = 0;
		while(iter.hasNext()){
			collection[index] = iter.next();
			index++;
		}
		double[][] pattern = new double[collection.length/s][s];
		for(int i=0; i<collection.length / s; i++){
			for(int j=0; j<s; j++){
				pattern[i][j] = collection[i * s + j];
			}
		}
		return pattern;
	}
	
	private int getIndexValue(String[] c, String tag){
		for(int i=0; i<c.length; i++){
			if(c[i].equals(tag)){
				return i;
			}
		}
		return -1;
	}
	
	private int getIndexValue(Cluster c, String tag){
		for(int i=0; i<c.getDim(); i++){
			if(c.getTag(i).equals(tag)){
				return i;
			}
		}
		return -1;
	}

}
