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
package it.polimi.deib.city_sensing_server.timeline;

public class GeneralTimelineStep {
	
	long start;
	long end;
	double mobily_anomaly;
	double social_activity;
	double mobily_activity;
	double social_sentiment;
	
	public long getStart() {
		return start;
	}
	public void setStart(long start) {
		this.start = start;
	}
	public long getEnd() {
		return end;
	}
	public void setEnd(long end) {
		this.end = end;
	}
	public double getMobily_anomaly() {
		return mobily_anomaly;
	}
	public void setMobily_anomaly(double mobile_anomaly) {
		this.mobily_anomaly = mobile_anomaly;
	}
	public double getSocial_activity() {
		return social_activity;
	}
	public void setSocial_activity(double social_activity) {
		this.social_activity = social_activity;
	}
	public double getMobily_activity() {
		return mobily_activity;
	}
	public void setMobily_activity(double mobily_activity) {
		this.mobily_activity = mobily_activity;
	}
	public double getSocial_sentiment() {
		return social_sentiment;
	}
	public void setSocial_sentiment(double social_sentiment) {
		this.social_sentiment = social_sentiment;
	}
	
}
