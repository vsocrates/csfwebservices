package edu.cwru.bmhinformatics.csfwebservices;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

public class SDAuxiliary {
	
private static final int[] DAYS_IN_MONTH = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
	
	public static int elapsedSeconds(String S, String E) {
		String[] startDate = (S.split(",")[0]).split("\\.|:"), startTime = (S.split(",")[1]).split("\\.|:"), 
				 endDate = (E.split(",")[0]).split("\\.|:"), endTime = (E.split(",")[1]).split("\\.|:");
		int startDay = Integer.parseInt(startDate[0], 10), 
			startMonth = Integer.parseInt(startDate[1], 10), 
			startYear = Integer.parseInt(startDate[2], 10),
			startHour = Integer.parseInt(startTime[0], 10), 
			startMin = Integer.parseInt(startTime[1], 10), 
			startSec = Integer.parseInt(startTime[2], 10);
		int endDay = Integer.parseInt(endDate[0], 10), 
			endMonth = Integer.parseInt(endDate[1], 10), 
			endYear = Integer.parseInt(endDate[2], 10),
			endHour = Integer.parseInt(endTime[0], 10), 
			endMin = Integer.parseInt(endTime[1], 10), 
			endSec = Integer.parseInt(endTime[2], 10);
		if(endYear < startYear || (endYear == startYear && endMonth < startMonth))
			return -elapsedSeconds(E, S);
		int num = endDay - startDay;
		while(endYear >= startYear && endMonth > startMonth) {
			num += DAYS_IN_MONTH[--endMonth - 1] 
					+ ((endMonth == 2 && (endYear % 4 == 0 && (endYear % 100 != 0 || endYear % 400 == 0))) ? 1 : 0);
			if(endMonth == 1) {endMonth += DAYS_IN_MONTH.length; endYear--;}
		}
		num = 24 * num + (endHour - startHour);
		num = 60 * num + (endMin - startMin);
		num = 60 * num + (endSec - startSec);
		return num;
	}
	
	public static String addSeconds(String T, int sec) {
		String[] startDate = (T.split(",")[0]).split("\\.|:"), startTime = (T.split(",")[1]).split("\\.|:");
		int startDay = Integer.parseInt(startDate[0], 10), 
			startMonth = Integer.parseInt(startDate[1], 10), 
			startYear = Integer.parseInt(startDate[2], 10),
			startHour = Integer.parseInt(startTime[0], 10), 
			startMin = Integer.parseInt(startTime[1], 10), 
			startSec = Integer.parseInt(startTime[2], 10);
		startSec += sec;
		if(startSec > 60) {
			startMin += (startSec / 60);
			startSec %= 60;
		}
		if(startMin > 60) {
			startHour += (startMin / 60);
			startMin %= 60;
		}
		if(startHour > 24) {
			startDay += (startMin / 24);
			startSec %= 24;
		}
		int m = DAYS_IN_MONTH[startMonth - 1] 
				+ ((startMonth == 2 && (startYear % 4 == 0 && (startYear % 100 != 0 || startYear % 400 == 0))) ? 1 : 0);
		while(startDay > m) {
			startDay -= m;
			if(++startMonth > 12) {
				startMonth = 1;
				startYear++;
			}
			m = DAYS_IN_MONTH[startMonth - 1] 
					+ ((startMonth == 2 && (startYear % 4 == 0 && (startYear % 100 != 0 || startYear % 400 == 0))) ? 1 : 0);
		}
		return (startDay < 10 ? "0" : "") + startDay 
				+ "." + (startMonth < 10 ? "0" : "") + startMonth 
				+ "." + (startYear < 10 ? "0" : "") + startYear 
				+ "," + (startHour < 10 ? "0" : "") + startHour 
				+ "." + (startMin < 10 ? "0" : "") + startMin 
				+ "." + (startSec < 10 ? "0" : "") + startSec;
	}
	
	public static final Comparator <String> CHRON = new Comparator <String> () {
		@Override 
		public int compare(String A, String B) {return elapsedSeconds(B, A);}
	};
	
	public static final Comparator <String[]> CHRON_PAIR = new Comparator <String[]> () {
		@Override 
		public int compare(String[] A, String[] B) {return elapsedSeconds(B[0], A[0]);}
	};
	
	//reads and writes JSON objects for access to intermediate files
	public static final JsonObject hashMapJSONifier(Map<String,String> mapObject) {
		JsonObjectBuilder returnObjectBuilder = Json.createObjectBuilder();
		
		for (Map.Entry<String, String> entry : mapObject.entrySet()) {
			returnObjectBuilder.add(entry.getKey(), entry.getValue());
		}
		
		JsonObject jsonObj = returnObjectBuilder.build();
		return jsonObj;	
	};
	
	//this one is for multiple hashmaps all together
	public static final JsonArray hashMapListJSONifier(ArrayList<LinkedHashMap<String,String>> mapObject) {
		JsonArrayBuilder returnArrayBuilder = Json.createArrayBuilder();
		
		for(Map<String,String> object : mapObject) {
			JsonObjectBuilder subObject = Json.createObjectBuilder();
			for (Map.Entry<String, String> entry : object.entrySet()) {
				//"Transducer Type":"RF9             RF10            RA1             RA2             RA3" 
				//shows up like this for some data, consolidated, 
				//but i'm not sure if thats the right thing to do
				//used trim and other RegEx stuff. 
				System.out.println("key: " + entry.getKey() + "value: " + entry.getValue());
				String[] valueWords = entry.getValue().split("\\s+");
				for (int i = 0; i < valueWords.length; i++) {
					valueWords[i] = valueWords[i].trim();
				}
				String trimmedWord = String.join(" ", valueWords);
				System.out.println("trimmed word: " + trimmedWord);
				subObject.add(entry.getKey(), trimmedWord);
			}
			returnArrayBuilder.add(subObject);
		}
		
		
		
		return returnArrayBuilder.build();
	};
}
