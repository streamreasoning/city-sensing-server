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
package it.polimi.deib.city_sensing_server.topic_network;

public class TopicNetworkLink implements Comparable<TopicNetworkLink>{

	private int source;
	private int target;
	private double value;

	public int getSource() {
		return source;
	}
	public void setSource(int source) {
		this.source = source;
	}
	public int getTarget() {
		return target;
	}
	public void setTarget(int target) {
		this.target = target;
	}
	public double getValue() {
		return value;
	}
	public void setValue(double value) {
		this.value = value;
	}

	@Override
	public boolean equals(Object other){
		TopicNetworkLink o = (TopicNetworkLink) other;
		//if((o.source.equals(source) && o.target.equals(target)) || (o.source.equals(target) && o.target.equals(source))){
		if(source == target) {
		return true;
		} else {
			return false;
		}
	}
	
	@Override
	public int compareTo(TopicNetworkLink arg0) {
		if(arg0.value < value)
			return -1;
		else if(arg0.value > value)
			return 1;
		return 0;
	}



}
