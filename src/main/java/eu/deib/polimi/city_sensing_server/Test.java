package eu.deib.polimi.city_sensing_server;

import java.io.IOException;

import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

public class Test extends ServerResource{

//	private Logger logger = LoggerFactory.getLogger(Test.class.getName());

	@Get
	public void dataServer(Representation rep) throws IOException {

		System.out.println("It Works!!!");
		
		this.getResponse().setStatus(Status.SUCCESS_CREATED);
		this.getResponse().setEntity("It Works!!!", MediaType.TEXT_PLAIN);
		this.getResponse().commit();
		this.commit();	
		this.release();

	}

}
