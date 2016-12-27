package com.hieuslash;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Utils {

	public static Map<String, Object> jsonMap(String json) {
    Map<String, Object> map = new HashMap<String, Object>();
		try {

			ObjectMapper mapper = new ObjectMapper();
			map = mapper.readValue(json, new TypeReference<Map<String, String>>(){});
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
      return map;
    }
	}

}
