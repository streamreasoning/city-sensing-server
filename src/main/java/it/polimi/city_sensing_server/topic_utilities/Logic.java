package it.polimi.city_sensing_server.topic_utilities;

import it.polimi.deib.city_sensing_server.configuration.Config;

import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class Logic {
	
	static CharsetEncoder asciiEncoder = Charset.forName("ISO-8859-1").newEncoder(); 
	
	public static boolean isPureAscii(String v) {
		return asciiEncoder.canEncode(v);
	}
	
	public class patternElement {
		private Date date;
		private int hour;
		private int occ;
		
		public void setDate(Date date){
			this.date = date;
		}
		
		public void setHour(int hour){
			this.hour = hour;
		}
		
		public void setOcc(int occ){
			this.occ = occ;
		}
		
		public Date getDate(){
			return this.date;
		}
		
		public int getHour(){
			return this.hour;
		}
		
		public int getOcc(){
			return this.occ;
		}
	}
	
	public class DateSlot {
		private Date startDate;
		private Date endDate;
		
		public DateSlot() {	
		}
		
		public void setStartDate(Date startDate){
			this.startDate = startDate;
		}
		
		public void setEndDate(Date endDate){
			this.endDate = endDate;
		}
		
		public Date getStartDate(){
			return this.startDate;
		}
		
		public Date getEndDate() {
			return this.endDate;
		}
		
		public int getDayDifference(){
			long diff = this.endDate.getTime() - this.startDate.getTime();
			int dayDiff = (int) (diff / 86400000);
			return dayDiff + 1;
		}
	}
	
	protected Connection conn;
	protected double[][] pattern;
	protected Date[] datePattern;
	protected DateSlot[] dateSlotPattern;
	protected double[][] aggregatePattern;
	protected DateFormat format = new SimpleDateFormat("dd/MM/yyyy"); 
	private Date maxDate;
	private Date minDate;
	
	public Logic() throws ClassNotFoundException, SQLException {
		String HOST = Config.getInstance().getTopicHostname();
		String PORT = Config.getInstance().getTopicPort();
		String USER = Config.getInstance().getTopicUsername();
		String PWD = Config.getInstance().getTopicPassword();
		String DB = Config.getInstance().getTopicDBName();
		Class.forName("org.postgresql.Driver");
		conn=DriverManager.getConnection("jdbc:postgresql://"+HOST+":"+PORT+"/"+DB, USER, PWD);
		minDate = getDateLimit(0);
		maxDate = getDateLimit(1);
	}
	
	public DateFormat getDateFormat() {
		return this.format;
	}
	
	public Date minDate(){
		return minDate;
	}
	
	public Date maxDate(){
		return maxDate;
	}
	
	public String[] getTopHashtag(Date startDate, Date endDate) throws SQLException, ParseException {
		ArrayList<String> topHashtag = new ArrayList<String>();
		String query ="select h.tag from hashtag h join occorrenza o on h.pk_tag = o.tag join tweet t on o.tweet = t.pk_tweet where t.day >= ? and t.day <= ? group by h.tag order by count(*) desc";
		PreparedStatement pstat = conn.prepareStatement(query);
		pstat.setDate(1, parseUtilDate(startDate));
		pstat.setDate(2, parseUtilDate(endDate));
		ResultSet rs = pstat.executeQuery();
		int count = 0;
		while(rs.next() && count < 100) {
			if(isPureAscii(rs.getString(1))){
				topHashtag.add(new String(rs.getString(1)));
				count++;
			}
		}
		return listToVector(topHashtag);
	}
	
	public void buildDatePattern(int dayMode, int dim){
		dateSlotPattern = new DateSlot[dim];
		for(int i=0; i<dateSlotPattern.length; i++){
			dateSlotPattern[i] = new DateSlot();
		}
		switch(dayMode) {
		case 0 : {
			for(int i=0; i<dateSlotPattern.length; i++){
				dateSlotPattern[i].setStartDate(datePattern[i]);
				dateSlotPattern[i].setEndDate(datePattern[i]);
			}
			break;
		}
		case 1 : {
			dateSlotPattern[0].setStartDate(datePattern[0]);
			int dSPInd = 0;
			for(int i=1; i<datePattern.length; i++){
				if((datePattern[i].getDay() == 0) || (datePattern[i].getDay() == 5)){
					dateSlotPattern[dSPInd].setEndDate(datePattern[i]);
					dSPInd++;
				} else if ((datePattern[i].getDay() == 6) || (datePattern[i].getDay() == 1)){
					dateSlotPattern[dSPInd].setStartDate(datePattern[i]);
				}
			}
			dateSlotPattern[dSPInd].setEndDate(datePattern[datePattern.length-1]);
			break;
		}
		case 2 : {
			dateSlotPattern[0].setStartDate(datePattern[0]);
			int dSPInd = 0;
			for(int i=1; i<datePattern.length; i++){
				if(datePattern[i].getDay() == 0){
					dateSlotPattern[dSPInd].setEndDate(datePattern[i]);
					dSPInd++;
				} else if (datePattern[i].getDay() == 1){
					dateSlotPattern[dSPInd].setStartDate(datePattern[i]);
				}
			}
			dateSlotPattern[dSPInd].setEndDate(datePattern[datePattern.length-1]);
			break;
		}
		case 3 : {
			dateSlotPattern[0].setStartDate(datePattern[0]);
			int dSPInd = 0;
			for(int i=1; i<datePattern.length-1; i++){
				if((datePattern[i+1].getDate() == 16) || (datePattern[i+1].getDate() == 1)){
					dateSlotPattern[dSPInd].setEndDate(datePattern[i]);
					dSPInd++;
				} else if ((datePattern[i].getDate() == 16) || (datePattern[i].getDate() == 1)){
					dateSlotPattern[dSPInd].setStartDate(datePattern[i]);
				}
			}
			dateSlotPattern[dSPInd].setEndDate(datePattern[datePattern.length-1]);
			break;
		}
		case 4 : {
			dateSlotPattern[0].setStartDate(datePattern[0]);
			int dSPInd = 0;
			for(int i=1; i<datePattern.length-1; i++){
				if(datePattern[i+1].getDate() == 1){
					dateSlotPattern[dSPInd].setEndDate(datePattern[i]);
					dSPInd++;
				} else if (datePattern[i].getDate() == 1){
					dateSlotPattern[dSPInd].setStartDate(datePattern[i]);
				}
			}
			dateSlotPattern[dSPInd].setEndDate(datePattern[datePattern.length-1]);
			break;
		}
		}
	}
	
	public void buildAggregatePattern(String hashtag, int dayMode, int timeMode, int d1, int d2) throws IndexOutOfBoundsException, SQLException{
		aggregatePattern = new double[d1][d2];
		extractPattern(hashtag);
		double[][] tempPattern = new double[d1][pattern[0].length];
		tempPattern = sumDays(dayMode, d1);
		aggregatePattern = sumHours(tempPattern, timeMode, d2);
	}
	
	public void buildAggregateMinutePattern(String hashtag, int startHour, int endHour, int minuteOffset) throws SQLException {
		extractMinutePattern(hashtag, startHour, endHour);
		aggregatePattern = new double[24][60 / minuteOffset];
		aggregatePattern = sumMinutes(minuteOffset);
	}
	
	private double[][] sumMinutes(int minuteOffset) {
		double[][] aggregate = new double[24][60 / minuteOffset];
		for(int i=0; i<24; i++){
			for(int j=0; j<60; j++){
				aggregate[i][j % minuteOffset] += pattern[i][j];
			}
		}
		return aggregate;
	}
	
	public double getAggregatePattern(int i, int j){
		return aggregatePattern[i][j];
	}
	
	public int getAggregatePatternLength(){
		return aggregatePattern.length;
	}
	
	public int getAggregatePatternNumRow(){
		return aggregatePattern[0].length;
	}
	
	public void buildAggregatePattern(String hashtag, Date startDate, Date endDate, int dayMode, int timeMode, int d1, int d2) throws IndexOutOfBoundsException, SQLException{
		aggregatePattern = new double[d1][d2];
		extractPattern(hashtag, startDate, endDate);
		double[][] tempPattern = new double[d1][pattern[0].length];
		tempPattern = sumDays(dayMode, d1);
		aggregatePattern = sumHours(tempPattern, timeMode, d2);
	}
	
	public void buildAggregateCumulativePattern(String[] hashtags, Date startDate, Date endDate, int dayMode, int timeMode, int d1, int d2) throws SQLException {
		aggregatePattern = new double[d1][d2];
		extractCumulativePattern(hashtags, startDate, endDate);
		double[][] tempPattern = new double[d1][pattern[0].length];
		tempPattern = sumDays(dayMode, d1);
		aggregatePattern = sumHours(tempPattern, timeMode, d2);
	}
	
	private void extractCumulativePattern(String[] hashtags, Date startDate, Date endDate) throws SQLException {
		buildEmptyPattern(startDate, endDate);
		String query = "select T.day, T.hour, count(distinct(T.tweet)) from occorrenza O join tweet T on O.tweet = T.pk_tweet where T.day >= ? and T.day <= ? and (O.tag = ?";
		for(int i=1; i<hashtags.length; i++){
			query += " or O.tag = ?";
		}
		query += ") group by T.day, T.hour";
		PreparedStatement pstat = conn.prepareStatement(query);
		pstat.setDate(1, parseUtilDate(startDate));
		pstat.setDate(2, parseUtilDate(endDate));
		for(int i=0; i<hashtags.length; i++){
			pstat.setInt(3+i, getHashtagCode(hashtags[i]));
		}
		ResultSet rs = pstat.executeQuery(); 
		while(rs.next()){
			pattern[getDateIndex(rs.getDate(1))][rs.getInt(2)] = rs.getInt(3);
		}
	}
	
	public class CooccourenceValueVector {
		private String tag;
		private double value;
		
		public CooccourenceValueVector(String tag, double value){
			this.tag = tag;
			this.value = value;
		}
		
		public void setTag(String tag){
			this.tag = tag;
		}
		
		public void setValue(double value){
			this.value = value;
		}
		
		public String getTag(){
			return this.tag;
		}
		
		public double getValue() {
			return this.value;
		}
	}
	
	public ArrayList<CooccourenceValueVector> getCooccourenceValueVector(String h1, String startDate, String endDate) throws SQLException, ParseException {
		ArrayList<CooccourenceValueVector> cvv = new ArrayList<CooccourenceValueVector>();
		String totalQuery = "select sum(occorrenze) from hitforday where tag = ? and day >=? and day <= ?";
		int totalValue = -1;;
		PreparedStatement totStat = conn.prepareStatement(totalQuery);
		totStat.setString(1, h1);
		totStat.setDate(2, parseUtilDate(format.parse(startDate)));				
		totStat.setDate(3, parseUtilDate(format.parse(endDate)));
		ResultSet Totrs = totStat.executeQuery();
		Totrs.next();
		totalValue = Totrs.getInt(1);
		String coocQuery = "select H1.tag, count (T1.pk_tweet) from (select T.pk_tweet from tweet T join occorrenza O on T.pk_tweet = O.tweet join hashtag H on O.tag = H.pk_tag where H.tag = ? and T.day >=? and T.day <=?) T1 join occorrenza O1 on T1.pk_tweet = O1.tweet join hashtag H1 on O1.tag = H1.pk_tag where H1.tag != ? group by H1.tag order by count(T1.pk_tweet)";
		PreparedStatement pstatCooc = null;
		pstatCooc = conn.prepareStatement(coocQuery);
		pstatCooc.setString(1, h1);
		pstatCooc.setDate(2, parseUtilDate(format.parse(startDate)));
		pstatCooc.setDate(3, parseUtilDate(format.parse(endDate)));
		pstatCooc.setString(4, h1);
		ResultSet rs = pstatCooc.executeQuery();
		while(rs.next()){
			double val = (double)(rs.getInt(2) / totalValue);
			cvv.add(new CooccourenceValueVector(rs.getString(1), val));
		}
		return cvv;
	}
	
	public void buildAggregateAuthorPattern(String author, int dayMode, int timeMode, int d1, int d2) throws IndexOutOfBoundsException, SQLException{
		aggregatePattern = new double[d1][d2];
		extractAuthorPattern(author);
		double[][] tempPattern = new double[d1][pattern[0].length];
		tempPattern = sumDays(dayMode, d1);
		aggregatePattern = sumHours(tempPattern, timeMode, d2);
	}
	
	public void buildAggregateAuthorPattern(String author, Date startDate, Date endDate, int dayMode, int timeMode, int d1, int d2) throws IndexOutOfBoundsException, SQLException {
		aggregatePattern = new double[d1][d2];
		extractAuthorPattern(author, startDate, endDate);
		double[][] tempPattern = new double[d1][pattern[0].length];
		tempPattern = sumDays(dayMode, d1);
		aggregatePattern = sumHours(tempPattern, timeMode, d2);
	}
	
	private double[][] sumDays(int dayMode, int d1) {
		double[][] summerizedPattern = new double[d1][pattern[0].length];
		switch(dayMode){
		case 0: {
			summerizedPattern = pattern;
			break; }
		case 1: {
			int d1Ind = 0;
			for(int i=0; i<pattern.length; i++){
				for(int j=0; j<pattern[0].length; j++){
					summerizedPattern[d1Ind][j] += pattern [i][j];
				}
				if((datePattern[i].getDay() == 5) || (datePattern[i].getDay() == 0)){
					d1Ind++;
				}
			}
			break;}
		case 2: {
			int d1Ind = 0;
			for(int i=0; i<pattern.length; i++){
				for(int j=0; j<pattern[0].length; j++){
					summerizedPattern[d1Ind][j] += pattern [i][j];
				}
				if((datePattern[i].getDay() == 0)){
					d1Ind++;
				}
			}
			break;}
		case 3: {
			int d1Ind = 0;
			for(int i=0; i<pattern.length; i++){
				if(((datePattern[i].getDate() == 1) || (datePattern[i].getDate() == 15)) && ( i != 0)){
					d1Ind++;
				}
				for(int j=0; j<pattern[0].length; j++){
					summerizedPattern[d1Ind][j] += pattern [i][j];
				}
			}
			break;}
		case 4: {
			int d1Ind = 0;
			for(int i=0; i<pattern.length; i++){
				if((datePattern[i].getDate() == 1) && (i != 0)){
					d1Ind++;
				}
				for(int j=0; j<pattern[0].length; j++){
					summerizedPattern[d1Ind][j] += pattern [i][j];
				}
			}
			break;}
		}
		
		return summerizedPattern;
	}
	
	public Date getDatePattern(int index){
		return datePattern[index];
	}
	
	protected double[][] sumHours(double[][] pattern, int timeMode, int d2){
		double[][] summerizedPattern = new double[pattern.length][d2];
		for(int i=0; i<summerizedPattern.length; i++){
			int d2Ind = 0;
			for(int j=0; j<24; j++){
				summerizedPattern[i][d2Ind] += pattern[i][j];
				switch (timeMode) {
				case 0: {d2Ind++; break;}
				case 1: {if ((j==7) || (j==11) || (j==14) || (j==19)) {d2Ind++; } break;}
				case 2: {if (j==12) {d2Ind++; }break;}
				case 3: {break;}
				}
			}
		}
		return summerizedPattern;
	}
	
	public String getParsedSlotDate(int index){
		String slotDate = null;
		slotDate = format.format(dateSlotPattern[index].getStartDate()) + 
				"-" +
				format.format(dateSlotPattern[index].getEndDate());
		return slotDate;
	}
	
	public String[] listToVector(ArrayList<String> list){
		String[] vector = new String[list.size()];
		for(int i=0; i<list.size(); i++){
			vector[i] = list.get(i);
		}
		return vector;
	}
	
	protected void extractAuthorPattern(String author, Date startDate, Date endDate) throws SQLException, IndexOutOfBoundsException {
		buildEmptyPattern(startDate, endDate);
		String query = "select T.day, T.hour, count(T.tweet) from tweet T join autore A on T.author = A.pk_auth where A.author = ? and T.day >= ? and T.day <= ? group by T.day, T.hour";
		PreparedStatement pstat = conn.prepareStatement(query);
		pstat.setString(1, author);
		pstat.setDate(2, parseUtilDate(startDate));
		pstat.setDate(3, parseUtilDate(endDate));
		ResultSet rs = pstat.executeQuery();
		while(rs.next()){
			pattern[getDateIndex(rs.getDate(1))][rs.getInt(2)] = rs.getInt(3);
		}
	}
	
	protected void extractPattern(String tag, Date startDate, Date endDate) throws SQLException, IndexOutOfBoundsException{
		buildEmptyPattern(startDate, endDate);
		String query = "select T.day, T.hour, count(O.tweet) from tweet T join occorrenza O on T.pk_tweet = O.tweet join hashtag H on O.tag = H.pk_tag where H.tag = ? and T.day >= ? and T.day <= ? group by T.day, T.hour";
		PreparedStatement pstat = conn.prepareStatement(query);
		pstat.setString(1, tag);
		pstat.setDate(2, parseUtilDate(startDate));
		pstat.setDate(3, parseUtilDate(endDate));
		ResultSet rs = pstat.executeQuery();
		while(rs.next()){
			pattern[getDateIndex(rs.getDate(1))][rs.getInt(2)] = rs.getInt(3);
		}
	}
	
	protected void extractMinutePattern(String tag, int startHour, int endHour) throws SQLException {
		buildEmptyMinutePattern();
		String query = "select T.minute, count(O.tweet) from tweet T join occorrenza O on T.pk_tweet = O.tweet join hashtag H on O.tag = H.pk_tag where H.tag = ? and T.hour = ? group by T.minute";
		PreparedStatement pstat = conn.prepareStatement(query);
		if(endHour > startHour){
			for(int i=startHour; i<=endHour; i++){
				pstat.setString(1, tag);
				pstat.setInt(2, i);
				ResultSet rs = pstat.executeQuery();
				while(rs.next()){
					pattern[i][rs.getInt(1)] = rs.getInt(2);
				}
			}
		} else {
			for(int i=startHour; i<24; i++){
				pstat.setString(1, tag);
				pstat.setInt(2, i);
				ResultSet rs = pstat.executeQuery();
				while(rs.next()){
					pattern[i][rs.getInt(1)] = rs.getInt(2);
				}
			}
			for(int i=0; i<=endHour; i++){
				pstat.setString(1, tag);
				pstat.setInt(2, i);
				ResultSet rs = pstat.executeQuery();
				while(rs.next()){
					pattern[i][rs.getInt(1)] = rs.getInt(2);
				}
			}
		}
	}
	
	private void buildEmptyMinutePattern() {
		pattern = new double[24][60];
		for(int i=0; i<pattern.length; i++){
			for(int j=0; j<pattern[0].length; j++){
				pattern[i][j] = 0.0;
			}
		}
	}

	protected int getDateIndex(java.sql.Date date){
		java.util.Date utilDate = parseSqlDate(date);
		for(int i=0; i<datePattern.length; i++){
			if(utilDate.compareTo(datePattern[i]) == 0){
				return i;
			}
		}
		return -1;
	}
	
	protected int getHourIndex(int hour, int timeMode){
		switch(timeMode) {
		case 0: {
			return hour;}
		case 1: {
			if(hour<8){
				return 0;
			} else if(hour < 12){
				return 1;
			} else if(hour < 15){
				return 2;
			} else if(hour < 20) {
				return 3;
			} else {
				return 4;
			}
			}
		case 2: {
			if(hour < 13){
				return 0;
			} else {
				return 1;
			}
			}
		case 3: {
			return 0;}
		}
		return -1;
	}
	
	private void buildEmptyPattern(Date startDate, Date endDate){
		pattern = new double[dayDifference(startDate, endDate) + 1][24];
		datePattern = new Date[pattern.length];
		datePattern[0] = startDate;
		Date currentDate = new Date(startDate.getTime());
		for(int i=1; i<datePattern.length; i++){
			currentDate.setTime(currentDate.getTime() + 86400000);
			datePattern[i] = new Date(currentDate.getTime());
		}
	}
	
	protected java.sql.Date parseUtilDate(java.util.Date utilDate){
		java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());
		return sqlDate;
	}
	
	protected java.util.Date parseSqlDate(java.sql.Date sqlDate){
		java.util.Date utilDate = new java.util.Date(sqlDate.getTime());
		return utilDate;
	}
	
	public int dayDifference(Date startDate, Date endDate){
		long diff = endDate.getTime() - startDate.getTime();
		return (int) (diff / 86400000);
	}
	
	public int getMaxDayDifference(){
		return dayDifference(minDate(), maxDate());
	}
	
	protected void extractAuthorPattern(String author) throws IndexOutOfBoundsException, SQLException{
		extractAuthorPattern(author, minDate(), maxDate());
	}
	
	protected void extractPattern(String tag) throws IndexOutOfBoundsException, SQLException{
		extractPattern(tag, minDate(), maxDate());
	}
	
	protected double[][] getPattern(){
		return this.pattern;
	}
	
	protected void resetPattern(){
		for(int i=0; i<pattern.length; i++){
			for(int j=0; j<pattern[0].length; j++){
				pattern[i][j] = 0.0;
			}
		}
	}
	
	protected int getHashtagCode(String tag) throws SQLException {
		/**
		 * Returns the primary-key code of the tag given by parameter
		 */
			
		String query = "select pk_tag from hashtag where tag = ?";
		PreparedStatement stat = conn.prepareStatement(query);
		stat.setString(1, tag);
		java.sql.ResultSet rs = stat.executeQuery();
		rs.next();
		return rs.getInt(1);
	}
	
	protected String getHashtagByCode(int code) throws SQLException {
		String query ="select tag from hashtag where pk_tag = ?";
		PreparedStatement pstat = conn.prepareStatement(query);
		pstat.setInt(1, code);
		ResultSet rs = pstat.executeQuery();
		while(rs.next()){
			return rs.getString(1);
		}
		return null;
	}
	
	public int getNumWeeks() throws SQLException {
		/**
		 * Returns the total number of weeks in the database
		 */
		String query = "select count(*) from settimana";
		Statement stat = conn.createStatement();
		ResultSet rs = stat.executeQuery(query);
		rs.next();
		return rs.getInt(1);
	}
	
	public Date getDateLimit(int flag) throws SQLException {
		/**
		 * flag = 0 --> data iniziale
		 * flag == 1 --> data finale
		 */
		String query = "select * from days";
		Statement stat = conn.createStatement();
		ResultSet rs = stat.executeQuery(query);
		if(flag == 0){
			rs.next();
			return rs.getDate(1);
		} else if (flag == 1){
			Date current = null;
			while(rs.next()){
				current = rs.getDate(1);
			}
			return current;
		} else {
			return null;
		}
	}
	
	public String[] getDateList() throws SQLException {
		ArrayList<String> dateList = new ArrayList<String>();
		String query = "select day from days order by day";
		Statement stat = conn.createStatement();
		ResultSet rs = stat.executeQuery(query);
		while(rs.next()){
			dateList.add(new String(format.format(rs.getDate(1))));
		}
		return listToVector(dateList);
	}
	
	public int getNumSlot() throws SQLException{
		/**
		 * Returns the total number of timeslot in the database
		 */
		String query = "select count(*) from timeslot";
		Statement stat = conn.createStatement();
		ResultSet rs = stat.executeQuery(query);
		rs.next();
		return rs.getInt(1);
	}
	
	public int[] getRgbCode(double value, double max){
		int[] rgb = new int[3];
		rgb[0] = 255;
		rgb[2] = rgb[1] = (int)(255 - ((value * 255) / max));
		return rgb;
	}
	
	public double[][] buildCumulativePattern(String[] hashtagList, int length, Date startDate, Date endDate, int d2, int timeMode) throws SQLException {
		double[] pattern = new double[length];
		for(int i=0; i<length; i++){
			pattern[i] = 0.0;
		}
		String query = "select T.day, T.hour, count(distinct(T.tweet)) from occorrenza O join tweet T on O.tweet = T.pk_tweet where T.day >= ? and T.day <= ? and (O.tag = ?";
		for(int i=1; i<hashtagList.length; i++){
			query += " or O.tag = ?";
		}
		query += ") group by T.day, T.hour";
		PreparedStatement pstat = conn.prepareStatement(query);
		pstat.setDate(1, parseUtilDate(startDate));
		pstat.setDate(2, parseUtilDate(endDate));
		for(int i=0; i<hashtagList.length; i++){
			pstat.setInt(3+i, getHashtagCode(hashtagList[i]));
		}
		ResultSet rs = pstat.executeQuery(); 
		while(rs.next()){
			pattern[getDateIndex(rs.getDate(1)) * 24 + rs.getInt(2)] = (double)(rs.getDouble(3));
		}
		double[][] tempPattern = getPattern(pattern, 24);
		return sumHours(tempPattern, timeMode, d2);
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
	
	public int[] setPatternDimension(int dayMode, int timeMode, String startDate, String endDate) throws ParseException {
		int[] dimensions = new int[2];
		int days = -1;
		Date start = null;
		Date end = null;
		days = dayDifference(format.parse(startDate), format.parse(endDate)) + 1;
		start = findWeekLimit(format.parse(startDate), 0);
		end = findWeekLimit(format.parse(endDate), 1);
		int weeks = (dayDifference(start, end) + 1) / 7;
		int month = -1;
		month = format.parse(endDate).getMonth() - format.parse(startDate).getMonth() + 1;
		int d1 = -1; int d2 = -1;
		switch (dayMode){
			case 0: {
				d1 = days;
			break; }
			case 1: {
				if((format.parse(startDate).getDay()==0) || (format.parse(startDate).getDay()==6)){
					if((format.parse(endDate).getDay()==0) || (format.parse(endDate).getDay()==6)){
						d1 = (weeks * 2) - 1;
					} else {
						d1 = (weeks * 2) - 2;
					}
				} else {
					if((format.parse(endDate).getDay()==0) || (format.parse(endDate).getDay()==6)){
						d1 = weeks * 2;
					} else {
						d1 = (weeks * 2) - 1;
					}
				}
			break; }
			case 2: {
				d1 = weeks;
			break; }
			case 3: {
				if((format.parse(startDate).getDate()>15) && (format.parse(endDate).getDate()<16)){
					d1 = (month * 2) - 2;
				} else if ((format.parse(startDate).getDate()>15) && (format.parse(endDate).getDate()>15)){
					d1 = (month * 2) - 1;
				} else if ((format.parse(startDate).getDate()<16) && (format.parse(endDate).getDate()<16)) {
					d1 = (month * 2) - 1;
				} else {
					d1 = month * 2;
				}
			break; }
			case 4: {
				d1 = month;
			break; }
		}
		switch (timeMode){
			case 0: {
				d2 = 24;
			break; }
			case 1: {
				d2 = 5;
			break; }
			case 2: {
				d2 = 2;
			break; }
			case 3: {
				d2 = 1;
			break; }
		}
		dimensions[0] = d1;
		dimensions[1] = d2;
		pattern = new double[d1][d2];
		return dimensions;
	}
	
	public int[] setPatternDimension(int dayMode, int timeMode) {
		int[] dimensions = new int[2];
		int days = getMaxDayDifference() + 1;
		Date start = findWeekLimit(minDate(), 0);
		Date end = findWeekLimit(maxDate(), 1);
		int weeks = (dayDifference(start, end) + 1) / 7;
		int month = maxDate().getMonth() - minDate().getMonth() + 1;
		int d1 = -1; int d2 = -1;
		switch (dayMode){
			case 0: {
				d1 = days;
			break; }
			case 1: {
				if((minDate().getDay()==0) || (minDate().getDay()==6)){
					if((maxDate().getDay()==0) || (maxDate().getDay()==6)){
						d1 = (weeks * 2) - 1;
					} else {
						d1 = (weeks * 2) - 2;
					}
				} else {
					if((maxDate().getDay()==0) || (maxDate().getDay()==6)){
						d1 = weeks * 2;
					} else {
						d1 = (weeks * 2) - 1;
					}
				}
			break; }
			case 2: {
				d1 = weeks;
			break; }
			case 3: {
				if((minDate().getDate()>15) && (maxDate().getDate()<16)){
					d1 = (month * 2) - 2;
				} else if ((minDate().getDate()>15) && (maxDate().getDate()>15)){
					d1 = (month * 2) - 1;
				} else if ((minDate().getDate()<16) && (maxDate().getDate()<16)) {
					d1 = (month * 2) - 1;
				} else {
					d1 = month * 2;
				}
			break; }
			case 4: {
				d1 = month;
			break; }
		}
		switch (timeMode){
			case 0: {
				d2 = 24;
			break; }
			case 1: {
				d2 = 5;
			break; }
			case 2: {
				d2 = 2;
			break; }
			case 3: {
				d2 = 1;
			break; }
		}
		dimensions[0] = d1;
		dimensions[1] = d2;
		pattern = new double[d1][d2];
		return dimensions;
	}
	
	private Date findWeekLimit(Date date, int mode){
		/**
		 * mode = 0 --> find start of the week
		 * mode = 1 --> find end of the week
		 */
		int dayOfWeek = date.getDay();
		Date tempDate = new Date(date.getTime());
		if (mode == 0){
			if(dayOfWeek == 0){
				tempDate.setTime(tempDate.getTime() - 6 * 86400000);
			} else {
				tempDate.setTime(tempDate.getTime() + (1 - dayOfWeek) * 86400000);
			}
			return new Date(tempDate.getTime());
		} else if (mode == 1){
			if(dayOfWeek != 0){
				tempDate.setTime(tempDate.getTime() + (7 - dayOfWeek) * 86400000);
			} 
			return new Date(tempDate.getTime());
		}
		return null;
	}
	
	public int[] getAllTimeSlotLimit(int timeMode){
		int[] allTimeSlot = null;
		switch(timeMode){
		case 0: {
			allTimeSlot = new int[48];
			for(int i=0; i<allTimeSlot.length-1; i=i+2){
				allTimeSlot[i] = i/2;
				allTimeSlot[i+1] =(i/2)+1; 
			}
			break;}
		case 1: {
			allTimeSlot = new int[10];
			allTimeSlot[0] = 0;
			allTimeSlot[1] = 7;
			allTimeSlot[2] = 7;
			allTimeSlot[3] = 11;
			allTimeSlot[4] = 11;
			allTimeSlot[5] = 14;
			allTimeSlot[6] = 14;
			allTimeSlot[7] = 19;
			allTimeSlot[8] = 19;
			allTimeSlot[9] = 24;
			break;}
		case 2: {
			allTimeSlot = new int[4];
			allTimeSlot[0] = 0;
			allTimeSlot[1] = 12;
			allTimeSlot[2] = 12;
			allTimeSlot[3] = 24;
			break;}
		case 3: {
			allTimeSlot = new int[2];
			allTimeSlot[0] = 0;
			allTimeSlot[1] = 24;
			break;}
		}
		return allTimeSlot;
	}
	
	public int[] getTimeSlotLimit(int timeSlot, int timeMode){
		int[] timeSlotLimit = new int[2];
		switch(timeMode){
		case 0:{
			timeSlotLimit[0] = timeSlot;
			timeSlotLimit[1] = timeSlot + 1;
			break;}
		case 1:{
			switch(timeSlot){
			case 0: {
				timeSlotLimit[0] = 0;
				timeSlotLimit[1] = 7;
				break;}
			case 1: {
				timeSlotLimit[0] = 7;
				timeSlotLimit[1] = 11;
				break;}
			case 2: {
				timeSlotLimit[0] = 11;
				timeSlotLimit[1] = 14;
				break;}
			case 3: {
				timeSlotLimit[0] = 14;
				timeSlotLimit[1] = 19;
				break;}
			case 4: {
				timeSlotLimit[0] = 19;
				timeSlotLimit[1] = 24;
				break;}
			}
			break;}
		case 2:{
			switch(timeSlot){
			case 0: {
				timeSlotLimit[0] = 0;
				timeSlotLimit[1] = 12;
				break;}
			case 1: {
				timeSlotLimit[0] = 12;
				timeSlotLimit[1] = 24;
				break;}
			}
			break;}
		case 3:{
			timeSlotLimit[0] = 0;
			timeSlotLimit[1] = 24;
			break;}
		}
	return timeSlotLimit;
	}

}
