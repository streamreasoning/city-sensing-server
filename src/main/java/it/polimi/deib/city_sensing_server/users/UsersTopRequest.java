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

import java.util.ArrayList;

public class UsersTopRequest {
	
	private String start;
	private String end;
	private ArrayList<String> cells;
	private String threshold;
	
	public UsersTopRequest() {
		super();
	}

	public UsersTopRequest(String start, String end, ArrayList<String> cells,
			String threshold) {
		super();
		this.start = start;
		this.end = end;
		this.cells = cells;
		this.threshold = threshold;
	}

	public String getStart() {
		return start;
	}

	public void setStart(String start) {
		this.start = start;
	}

	public String getEnd() {
		return end;
	}

	public void setEnd(String end) {
		this.end = end;
	}

	public ArrayList<String> getCells() {
		return cells;
	}

	public void setCells(ArrayList<String> cells) {
		this.cells = cells;
	}

	public String getThreshold() {
		return threshold;
	}

	public void setThreshold(String threshold) {
		this.threshold = threshold;
	}
	
}
