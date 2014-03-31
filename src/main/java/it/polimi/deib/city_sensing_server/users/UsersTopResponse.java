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
