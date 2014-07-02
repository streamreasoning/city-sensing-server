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
