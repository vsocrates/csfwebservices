package edu.cwru.bmhinformatics.csfwebservices;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("clinicalevent")
public class ClinicalEventAnnotationResource {

	/**
	 * 
	 * RESTful Resource methods
	 * 
	 */
	
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String getAllClinicalEvents() {
		
		
		
		return "Got Clinical Events";
	}
	
	
	
	/**
	 * 
	 * Auxiliary methods
	 * 
	 */
	

	public static LinkedHashMap <String, String> EDFAnnotationExtractor(File annotationFile) {
		
		LinkedHashMap <String, String> clinicalEventMap = new LinkedHashMap <String, String>();
		String buffStr = "";	
		
		try {															// BEGIN Try/Catch Block to Read Files
			/**
			 * PHASE II: EXTRACT THE CLINICAL EVENT ANNOTATIONS
			 */
			BufferedReader annotationFileReader 
					= new BufferedReader(new FileReader(annotationFile)); 	// Open the clinical event annotation file and wrap a bufferedreader 
		
			while((buffStr = annotationFileReader.readLine()) != null) { 	// In a while loop, read in the <timestamp, clinical event> - each in separate line
				String[] strArr = buffStr.split("\\s+", 2);						// Split the string to get two substrings (timestamp, clinical event) - controlled by number of times a regex is applied
				if(strArr.length >= 2)
					clinicalEventMap.put(strArr[0], strArr[1].trim());
			}
			annotationFileReader.close();									// Close the bufferedreader
		} 
		catch(IOException e) {
			System.out.println("IOException: "); 
			e.printStackTrace();
		}	
		return clinicalEventMap;
	}
	
}
