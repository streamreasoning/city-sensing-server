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

import it.polimi.deib.city_sensing_server.users.utilities.User;

import java.util.ArrayList;
import java.util.Collection;

public class UsersTopResponse {
	
	private Collection<User> users;

	public UsersTopResponse() {
		users = new ArrayList<User>();
	}

	public UsersTopResponse(Collection<User> users) {
		super();
		this.users = users;
	}

	public Collection<User> getUsers() {
		return users;
	}

	public void setUsers(Collection<User> users) {
		this.users = users;
	}
	
	public void addElementToList(User user){
		users.add(user);
	}
	
	public void addElementsToList(Collection<User> userList){
		users.addAll(userList);
	}

}
