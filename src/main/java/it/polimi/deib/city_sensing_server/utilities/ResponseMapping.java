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

public class ResponseMapping {
	
	private long cell_id;
	private long start_ts;
	private double call_in;
	private double call_out;
	private double rec_in;
	private double rec_out;
	private double sms_in;
	private double sms_out;
	private double internet;
	private double anomaly;
	private double social_activity;
	private double social_sentiment;
	
	public ResponseMapping() {
		super();
		this.cell_id = 0;
		this.start_ts = 0;
		this.call_in = 0;
		this.call_out = 0;
		this.rec_in = 0;
		this.rec_out = 0;
		this.sms_in = 0;
		this.sms_out = 0;
		this.internet = 0;
		this.anomaly = 0;
		this.social_activity = 0;
		this.social_sentiment = 0;
	}

	public ResponseMapping(long cell_id, long start_ts, double call_in,
			double call_out, double rec_in, double rec_out, double sms_in,
			double sms_out, double internet, double anomaly, double social_activity, double social_sentiment) {
		super();
		this.cell_id = cell_id;
		this.start_ts = start_ts;
		this.call_in = call_in;
		this.call_out = call_out;
		this.rec_in = rec_in;
		this.rec_out = rec_out;
		this.sms_in = sms_in;
		this.sms_out = sms_out;
		this.internet = internet;
		this.anomaly = anomaly;
		this.social_activity = social_activity;
		this.social_sentiment = social_sentiment;
	}

	public long getCell_id() {
		return cell_id;
	}

	public void setCell_id(long cell_id) {
		this.cell_id = cell_id;
	}

	public long getStart_ts() {
		return start_ts;
	}

	public void setStart_ts(long start_ts) {
		this.start_ts = start_ts;
	}

	public double getCall_in() {
		return call_in;
	}

	public void setCall_in(double call_in) {
		this.call_in = call_in;
	}

	public double getCall_out() {
		return call_out;
	}

	public void setCall_out(double call_out) {
		this.call_out = call_out;
	}

	public double getRec_in() {
		return rec_in;
	}

	public void setRec_in(double rec_in) {
		this.rec_in = rec_in;
	}

	public double getRec_out() {
		return rec_out;
	}

	public void setRec_out(double rec_out) {
		this.rec_out = rec_out;
	}

	public double getSms_in() {
		return sms_in;
	}

	public void setSms_in(double sms_in) {
		this.sms_in = sms_in;
	}

	public double getSms_out() {
		return sms_out;
	}

	public void setSms_out(double sms_out) {
		this.sms_out = sms_out;
	}

	public double getInternet() {
		return internet;
	}

	public void setInternet(double internet) {
		this.internet = internet;
	}

	public double getAnomaly() {
		return anomaly;
	}

	public void setAnomaly(double anomaly) {
		this.anomaly = anomaly;
	}

	public double getSocial_activity() {
		return social_activity;
	}

	public void setSocial_activity(double social_activity) {
		this.social_activity = social_activity;
	}

	public double getSocial_sentiment() {
		return social_sentiment;
	}

	public void setSocial_sentiment(double social_sentiment) {
		this.social_sentiment = social_sentiment;
	}
	

}
