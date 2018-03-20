package edu.cwru.bmhinformatics.csfwebservices;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.regex.Pattern;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

@Path("clinicalevent")
public class ClinicalEventAnnotationResource {

	/**
	 * 
	 * RESTful Resource methods
	 * 
	 */
	
	@POST
	@Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
	public String getAllClinicalEvents(String data) {
		
		JsonReader jsonParamsReader = Json.createReader(new StringReader(data));
		JsonObject inputParams = jsonParamsReader.readObject();

		String edfFileListDir = inputParams.getString("edfFileDir");
		File edfFileList = new File(edfFileListDir);
		
		HashMap <String[], File> edfMap = new HashMap<>();
		for(File edfFile: edfFileList.listFiles()) {
			if (edfFile.getPath().indexOf(".edf") == -1) continue;								// Skip the .txt file and process only .edf files
			String edfStart = "";
			double records = 0, duration = 0;
			try {
				RandomAccessFile edfFileReader = new RandomAccessFile(edfFile, "r"); 				// Open a random access file reader
				edfFileReader.seek(168);
				byte[] buffer = new byte[8];
				edfFileReader.readFully(buffer);
				edfStart += new String(buffer).trim();
				buffer = new byte[8];
				edfFileReader.readFully(buffer);
				edfStart += ("," + new String(buffer).trim());
				edfFileReader.skipBytes(52);
				buffer = new byte[8];
				edfFileReader.readFully(buffer);
				records = Double.parseDouble(new String(buffer).trim());
				buffer = new byte[8];
				edfFileReader.readFully(buffer);
				duration = Double.parseDouble(new String(buffer).trim());
				edfFileReader.close();
			} catch(IOException e) {
				System.out.println("IOException: "); 
				e.printStackTrace();
			}																					// ENDOF Try/Catch Block to read files
			String[] startEnd = {edfStart, SDAuxiliary.addSeconds(edfStart, (int) (duration * records))};
			edfMap.put(startEnd, edfFile);
			edfStart = "";
		}
		
		File csfDir = new File(edfFileList.getPath() + "/" + edfFileList.getName() + "-CSF");
		if(csfDir.mkdir() || csfDir.exists()) System.out.println("Directory " + csfDir.getName() + " created");
		ArrayList <String[]> edfTimes = new ArrayList<>(edfMap.keySet());
		Collections.sort(edfTimes, SDAuxiliary.CHRON_PAIR);
		for(String[] A: edfTimes) System.out.println(Arrays.toString(A) + " " + edfMap.get(A));
		int fileCounter = 0, fragmentIndex = 0, totalFragments = 0;
		LinkedHashSet <String> termSet = new LinkedHashSet<>();

		int counter = 0;
		for(String[] T: edfTimes) {
			File edfFile = edfMap.get(T);
			File clinicalAnnotationFile 														// Grab the associated annotation .txt file
				= new File(edfFile.getPath().replace(".edf", ".txt"));
			if (!clinicalAnnotationFile.exists()) continue;
			System.out.println("Annotation file: " 
					+ clinicalAnnotationFile.toString() + "\n");							
			LinkedHashMap<String, String> clinicalAnnotations 							// Retrieve the study specific metadata and clinical event annotations 
					= EDFAnnotationExtractor(clinicalAnnotationFile);// - call the EDFStudyMetadataExtractor method
			termSet.addAll(clinicalAnnotations.values());
			
			System.out.println("size: " + clinicalAnnotations.size());
			
			JsonObject jsonifiedClinicalAnnotations = SDAuxiliary.hashMapToJSON(clinicalAnnotations);
			System.out.println("clnical events: " + jsonifiedClinicalAnnotations.toString());
//			System.out.println("test: " + Integer.toString(counter));
			File metadataFile = new File(csfDir, edfFile.getName().substring(0, edfFile.getName().length() - 4) +"_annotations.json");
			try {
				BufferedWriter fileWriter = new BufferedWriter(new FileWriter(metadataFile));
				fileWriter.write(jsonifiedClinicalAnnotations.toString());
				fileWriter.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			counter++;
		}
		
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
				if(strArr.length >= 2) {
					String[] splitWords = strArr[1].trim().split("\u0000+");
					
					String spacedWord = "";
					for(String word : splitWords) {
						spacedWord += word.trim() + " ";
					}
					clinicalEventMap.put(strArr[0], spacedWord.trim());
				}
					
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
