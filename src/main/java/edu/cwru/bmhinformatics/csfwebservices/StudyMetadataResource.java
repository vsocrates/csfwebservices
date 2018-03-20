package edu.cwru.bmhinformatics.csfwebservices;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("studymetadata")
public class StudyMetadataResource {

	/**
	 * 
	 * RESTful Resource methods
	 * 
	 */
	
	@POST
	@Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
	@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
	public Response processAllEDFFiles(String data) {
		
		JsonReader jsonParamsReader = Json.createReader(new StringReader(data));
		JsonObject inputParams = jsonParamsReader.readObject();

		String edfFileListDir = inputParams.getString("edfFileDir");
		File edfFileList = new File(edfFileListDir);
		
		HashMap <String[], File> edfMap = new HashMap<>();
		File csfDir = new File(edfFileList.getPath() + "/" + edfFileList.getName() + "-CSF");
		if(csfDir.mkdir() || csfDir.exists()) System.out.println("Directory " + csfDir.getName() + " created");

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
		
		URI createdResource = null;
		ArrayList <String[]> edfTimes = new ArrayList<>(edfMap.keySet());
		Collections.sort(edfTimes, SDAuxiliary.CHRON_PAIR);
		int fileCounter = 0, fragmentIndex = 0, totalFragments = 0;
		LinkedHashSet <String> termSet = new LinkedHashSet<>();
		ArrayList<LinkedHashMap<String,String>> allFiles = new ArrayList<LinkedHashMap<String, String>>();
		for(String[] T: edfTimes) {
			File edfFile = edfMap.get(T);
			LinkedHashMap <String, String> studyMetadata = EDFStudyMetadataExtractor(edfFile);// - call the EDFStudyMetadataExtractor method
			allFiles.add(studyMetadata);
			JsonObject jsonifiedStudyMetadata = SDAuxiliary.hashMapToJSON(studyMetadata);
			File metadataFile = new File(csfDir, edfFile.getName().substring(0, edfFile.getName().length() - 4) +"_studymetadata.json");
			createdResource = metadataFile.toURI();
			try {
				BufferedWriter fileWriter = new BufferedWriter(new FileWriter(metadataFile));
				fileWriter.write(jsonifiedStudyMetadata.toString());
				fileWriter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
				
		return Response.created(createdResource).build();
	}

	@GET
	@Path("studymetadatum/{filename}")
	@Produces(MediaType.TEXT_PLAIN)
	public String processOneEDFFile(@PathParam("filename") String filename) {
		//{MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON}
		File edfFileList = new File("C:\\Users\\Vimig\\Downloads\\CSF_Example\\CSF_Example");
//		File edfFileList = new File(args[0]);
//		System.out.println("file path arg:" + args[0] + "\n");		
		
		HashMap <String[], File> edfMap = new HashMap<>();
		File edfFile = new File(filename);
		if (edfFile.getPath().indexOf(".edf") == -1) return "";								// Skip the .txt file and process only .edf files
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
		
		File csfDir = new File(edfFileList.getPath() + "/" + edfFileList.getName() + "-CSF");
		if(csfDir.mkdir() || csfDir.exists()) System.out.println("Directory " + csfDir.getName() + " created");
		ArrayList <String[]> edfTimes = new ArrayList<>(edfMap.keySet());
		Collections.sort(edfTimes, SDAuxiliary.CHRON_PAIR);
//		for(String[] A: edfTimes) System.out.println(Arrays.toString(A) + " " + edfMap.get(A));
		int fileCounter = 0, fragmentIndex = 0, totalFragments = 0;
		LinkedHashSet <String> termSet = new LinkedHashSet<>();
		ArrayList<LinkedHashMap<String,String>> allFiles = new ArrayList<LinkedHashMap<String, String>>();
		int counter = 0;
		for(String[] T: edfTimes) {
			File edfFile2 = edfMap.get(T);
//			System.out.println("EDF file: " + edfFile.toString() + "\n");
//			File clinicalAnnotationFile 														// Grab the associated annotation .txt file
//					= new File(edfFile.getPath().replace(".edf", ".txt"));
//			System.out.println("Annotation file: " 
//					+ clinicalAnnotationFile.toString() + "\n");							
			LinkedHashMap <String, String> studyMetadata = EDFStudyMetadataExtractor(edfFile2);// - call the EDFStudyMetadataExtractor method
			allFiles.add(studyMetadata);
			JsonObject jsonifiedStudyMetadata = SDAuxiliary.hashMapToJSON(studyMetadata);
//			System.out.printlnw("test: " + Integer.toString(counter));
			File metadataFile = new File(csfDir, Integer.toString(counter) +".json");
			try {
				BufferedWriter fileWriter = new BufferedWriter(new FileWriter(metadataFile));
				fileWriter.write(jsonifiedStudyMetadata.toString());
				fileWriter.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			counter++;
		}
		
//		System.out.println("signal metadata" + allFiles + " " + Integer.toString(allFiles.size()));
		
		return "DONE TWO!";

	}
	
	/**
	 * 
	 * Auxiliary methods
	 * 
	 */
	
	
	/** 
	 * Extract study-specific metadata fields from EDF file
	 * @param: EDF file, Clinical annotation file
	 * @result: ArrayList with study-specific metadata
	 * 
	 */
	public static LinkedHashMap <String, String> EDFStudyMetadataExtractor(File edfFile) {
		LinkedHashMap <String, String> studyMetadata = new LinkedHashMap <String, String>();
		byte[] valueBuffer;
		
		try {												// BEGIN Try/Catch Block to Read Files
			/**
			 * PHASE I: EXTRACT THE STUDY-SPECIFIC METADATA
			 */
			studyMetadata.put("FileName", edfFile.getName()); 	// Add the filename to the hashtable 
			RandomAccessFile edfFileReader 
					= new RandomAccessFile(edfFile, "r"); 		// Open a random access file reader
			edfFileReader.seek(0);
			
			/** 
			 * Extract Patient Specific Metadata
			**/
			valueBuffer = new byte[8];							// Initialize the byte buffer with specific number of bytes
			edfFileReader.readFully(valueBuffer);				// Read in the EDF version (8 bytes)
			studyMetadata.put("Data Format Version", 			// Add the version to the hashtable
					new String(valueBuffer).trim());			
	
			/** 
			 * EDF+ spec for patient ID
			 * The 'local patient identification' field must start with the subfields (subfields do not contain, but are separated by, spaces):
			 * - the code by which the patient is known in the hospital administration.
			 * - sex (English, so F or M).
			 * - birthdate in dd-MMM-yyyy format using the English 3-character abbreviations of the month in capitals. 02-AUG-1951 is OK, while 2-AUG-1951 is not.
			 * - the patients name. 
			 * 
			 */
			valueBuffer = new byte[80];							// Re-Initialize the byte buffer with specific number of bytes
			edfFileReader.read(valueBuffer);					// Read in the Patient ID (80 bytes)
			studyMetadata.put("Local Patient Identification", 	// Add the local patient identification to the hashtable (EDF+ specs include Gender and DOB)
					new String(valueBuffer).trim());			
	
			/**
			 * The 'local recording identification' field must start with the subfields (subfields do not contain, but are separated by, spaces):
			 * - The text 'Startdate'.
			 * - The startdate itself in dd-MMM-yyyy format using the English 3-character abbreviations of the month in capitals.
			 * - The hospital administration code of the investigation, i.e. EEG number or PSG number.
			 * - A code specifying the responsible investigator or technician.
			 * - A code specifying the used equipment. 
			 * 
			 */
			valueBuffer = new byte[80];							// Re-Initialize the byte buffer with specific number of bytes
			edfFileReader.read(valueBuffer);					// Read in the Recording ID (80 bytes)
			studyMetadata.put("Local Recording Identification", // Add the version to the hashtable
					new String(valueBuffer).trim());			
			
			/**
			 * EDF+ specs: The 'startdate' and 'starttime' fields in the header should contain only characters 0-9, 
			 * and the period (.) as a separator, for example "02.08.51". In the 'startdate', use 1985 as
			 * a clipping date in order to avoid the Y2K problem. So, the years 1985-1999 must be represented
			 * by yy=85-99 and the years 2000-2084 by yy=00-84. After 2084, yy must be 'yy' and only item 4
			 * of this paragraph defines the date. 
			 */
			valueBuffer = new byte[8];							// Re-Initialize the byte buffer with specific number of bytes
			edfFileReader.read(valueBuffer);					// Read in the Start Date (8 bytes) MM.DD.YY
			studyMetadata.put("Recording Start Date", 			// Add the recording start date to the hashtable
					new String(valueBuffer).trim());			
			
			valueBuffer = new byte[8];							// Re-Initialize the byte buffer with specific number of bytes HH:MM:SS
			edfFileReader.read(valueBuffer);					// Read in the Start Time (8 bytes)
			studyMetadata.put("Recording Start Time", 			// Add the recording start time to the hashtable
					new String(valueBuffer).trim());			
			
			
			
			valueBuffer = new byte[8];							// Re-Initialize the byte buffer with specific number of bytes HH:MM:SS
			edfFileReader.read(valueBuffer);					// Get the total number of bytes in EDF file header
			studyMetadata.put("Number of Header Bytes", 		// Add the header bytes to the hashtable
					new String(valueBuffer).trim());
					
			edfFileReader.skipBytes(44);						// Skip next 44 bytes (RESERVED)
			
			/**
			 * EDF+ Spec: The 'number of data records' can only be -1 during recording. As soon as the 
			 * file is closed, the correct number is known and must be entered. 
			 */
			valueBuffer = new byte[8];							// Re-Initialize the byte buffer with specific number of bytes - total number of records
			edfFileReader.read(valueBuffer);					// Read in the Number of Records (8 bytes)
			studyMetadata.put("Number of Data Records", 		// Add the number of data records to the hashtable
					new String(valueBuffer).trim());
			
			valueBuffer = new byte[8];							// Re-Initialize the byte buffer with specific number of bytes - duration of data records in seconds
			edfFileReader.read(valueBuffer);					// Read in the duration of data records (8 bytes)
			studyMetadata.put("Data Records Duration", 			// Add the duration of data records to the hashtable
					new String(valueBuffer).trim());
			
			valueBuffer = new byte[4];							// Re-Initialize the byte buffer with specific number of bytes - total number of signals in data record
			edfFileReader.read(valueBuffer);					// Read in the Number of Signals in Data Records (4 bytes)
			studyMetadata.put("Number of Signals", 				// Add the number of signals per data records to the hashtable
					new String(valueBuffer).trim());
			
			String endOfFragment 
					= SDAuxiliary.addSeconds(
							studyMetadata.get("Recording Start Date") + "," + studyMetadata.get("Recording Start Time"), 
							(int) (Double.parseDouble(studyMetadata.get("Number of Data Records")) * Double.parseDouble(studyMetadata.get("Data Records Duration"))));
			String[] endTokens = endOfFragment.split(",");
			studyMetadata.put("Recording End Date", endTokens[0]);
			studyMetadata.put("Recording End Time", endTokens[1]);

			edfFileReader.close();								// Close the random file reader
		} 
		catch(IOException e) {
			System.out.println("IOException: "); 
			e.printStackTrace();
		}													// ENDOF Try/Catch Block to read files
		return studyMetadata;								// RETURN the populated ArrayList
	}
	
}
