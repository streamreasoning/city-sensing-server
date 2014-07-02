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
package it.polimi.deib.city_sensing_server.users.utilities;

import java.util.ArrayList;

public class User4Topic {

	private String URI;
	private ArrayList<String> topics;
	private boolean alreadyChecked = false;
	
	public User4Topic() {
		topics = new ArrayList<String>();
	}
	public String getURI() {
		return URI;
	}
	public void setURI(String uRI) {
		URI = uRI;
	}
	public ArrayList<String> getTopics() {
		return topics;
	}
	public void setTopics(ArrayList<String> topic) {
		this.topics = topic;
	}

	public void addElement(String topic){
		this.topics.add(topic);
	}

	public int isIn(ArrayList<String> list){
		int cont = 0;
		for(String s : list){
			if(topics.contains(s))
				cont++;
		}
		return cont;
	}
	
	public void setAlreadyChecked(){
		alreadyChecked = true;
	}
	public boolean isAlreadyChecked() {
		return alreadyChecked;
	}
	public void setAlreadyChecked(boolean alreadyChecked) {
		this.alreadyChecked = alreadyChecked;
	}
		
}
