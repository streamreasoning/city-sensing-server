package it.polimi.deib.city_sensing_server.concept_flows;

import java.lang.reflect.Type;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class ConceptFlowsNodeSerializer implements JsonSerializer<ConceptFlowsNode>{

	public JsonElement serialize(ConceptFlowsNode src, Type typeOfSrc,JsonSerializationContext context) {
		
		JsonObject retValue = new JsonObject();
		
		retValue.addProperty("id", src.getId());
		retValue.addProperty("label", src.getLabel());
		retValue.addProperty("group", src.getGroup());
		
		if(src.getGroup().equals("hashtag") || src.getGroup().equals("Hashtag"))
			retValue.addProperty("sentiment", src.getSentiment());

		return retValue;
	}

}
