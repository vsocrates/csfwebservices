package edu.cwru.bmhinformatics.csfwebservices;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

import javax.json.JsonArray;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("channel")
public class ChannelMetadataResource {

	/**
	 * 
	 * RESTful Resource methods
	 * 
	 */
	
	@POST
	@Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
	@Path("metadata/{numChannels}")
	public String getChannelMetadata(@PathParam("numChannels") int numChannels) {
		
		//{MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON}
		File edfFileList = new File("/Users/vsocrates/Documents/School/BMHI/test_signals/CSF_Example");
		//File edfFileList = new File(args[0]);
//		System.out.println("file path arg:" + args[0] + "\n");		
		
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
//		for(String[] A: edfTimes) System.out.println(Arrays.toString(A) + " " + edfMap.get(A));
		int fileCounter = 0, fragmentIndex = 0, totalFragments = 0;
		LinkedHashSet <String> termSet = new LinkedHashSet<>();
		ArrayList<ArrayList<LinkedHashMap <String, String>>> allFiles = new ArrayList<ArrayList<LinkedHashMap <String, String>>>();
		int counter = 0;
		for(String[] T: edfTimes) {
			File edfFile = edfMap.get(T);
//			System.out.println("EDF file: " + edfFile.toString() + "\n");
//			File clinicalAnnotationFile 														// Grab the associated annotation .txt file
//					= new File(edfFile.getPath().replace(".edf", ".txt"));
//			System.out.println("Annotation file: " 
//					+ clinicalAnnotationFile.toString() + "\n");							
			ArrayList<LinkedHashMap <String, String>> channelMetadata = EDFChannelMetadataExtractor(edfFile, numChannels);// - call the EDFStudyMetadataExtractor method
			allFiles.add(channelMetadata);
			JsonArray jsonifiedChannelMetadata = SDAuxiliary.hashMapListJSONifier(channelMetadata);
//			System.out.println("test: " + Integer.toString(counter));
			File metadataFile = new File(csfDir, Integer.toString(counter) +"_channelmetadata.json");
			try {
				BufferedWriter fileWriter = new BufferedWriter(new FileWriter(metadataFile));
				fileWriter.write(jsonifiedChannelMetadata.toString());
				fileWriter.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			counter++;
		}
		
//		System.out.println("signal metadata" + allFiles + " " + Integer.toString(allFiles.size()));
		
		return "Got Channel Metadata";
		
	}
	
	/**
	 * 
	 * Auxiliary methods
	 * 
	 */
	
	
	/** 
	 * Extract channel-specific metadata fields from EDF file
	 * @param: EDF file, channelID, total number of channels
	 * @result: ArrayList with channel-specific metadata
	 * 
	 */
	public static ArrayList <LinkedHashMap <String, String>> EDFChannelMetadataExtractor(File edfFile, int numChannels){
		byte[] valueBuffer;
		ArrayList <LinkedHashMap <String, String>> channelMetadata = new ArrayList <LinkedHashMap <String, String>>();
		try {													// BEGIN Try/Catch Block to Read File
			RandomAccessFile edfFileReader 
					= new RandomAccessFile(edfFile, "r");			// Initialize a random file reader on the EDF file
						
			/**
			 * EDF+ spec for "label": Use the standard texts and polarity rules at http://www.edfplus.info/specs/edftexts.html. 
			 * These standard texts may in the future be extended with further texts, a.o. for Sleep scorings, 
			 * ENG and various evoked potentials. 
			 * 
			 */
			for(int channelId = 0; channelId < numChannels; channelId++) {
				LinkedHashMap <String, String> channelSpecificMetadata 
						= new LinkedHashMap <String, String>();			// Initialize the hashtable to store channel-specific metadata for the current channel
				
				int fileOffset = 256;									// Initialize fileOffset to 256 corresponding to length of study-specific metadata (8 + 80 + 80 + 8 + 8 + 8 + 44 + 8 + 8 + 4 bytes - EDF specs)
				valueBuffer = new byte[16];								// Initialize the byte buffer to read in the label attribute (16 bytes), e.g.EEG Fpz-Cz or Body temp
				edfFileReader.seek(fileOffset + (channelId * 16));		// Skip the bytes for study-specific metadata (256 bytes) + number of channels (each channel header occupies 16 byte for label attribute)
				edfFileReader.readFully(valueBuffer);					// Read in the label attribute
				channelSpecificMetadata.put("Label", 					// Add the label attribute-value to the hashtable
						new String(valueBuffer).trim());
				
				fileOffset = fileOffset + (numChannels * 16);			// Increment the fileOffset
				valueBuffer = new byte[80];								// Re-initialize the byte buffer - for transducer type (80 bytes), e.g. AgAgCl electrode
				edfFileReader.seek(fileOffset + (channelId * 80));		// Position the file pointer to appropriate channel-specific location for attribute "Transducer type"
				edfFileReader.readFully(valueBuffer);					// Read in the transducer type
				channelSpecificMetadata.put("Transducer Type", 			// Add the transducer type to the hashtable
						new String(valueBuffer).trim());
				
				fileOffset = fileOffset + (numChannels * 80);			// Increment the fileOffset
				valueBuffer = new byte[8];								// Re-initialize the byte buffer - physical dimension (8 bytes), e.g. uV or degreeC
				edfFileReader.seek(fileOffset + (channelId * 8));		// Position the file pointer to appropriate channel-specific location for attribute "physical dimension"
				edfFileReader.readFully(valueBuffer);					// Read in the physical dimension
				channelSpecificMetadata.put("Physical Dimension", 		// Add the physical dimension to the hashtable 
						new String(valueBuffer).trim());
				
				fileOffset = fileOffset + (numChannels * 8);			// Increment the fileOffset
				valueBuffer = new byte[8];								// Re-initialize the byte buffer - physical minimum (8 bytes), e.g. -500 or 34
				edfFileReader.seek(fileOffset + (channelId * 8));		// Position the file pointer to appropriate channel-specific location for attribute "physical minimum"
				edfFileReader.readFully(valueBuffer);					// Read in the physical minimum
				channelSpecificMetadata.put("Physical Minimum", 		// Add the physical minimum to the hashtable
						new String(valueBuffer).trim());
				
				fileOffset = fileOffset + (numChannels * 8);			// Increment the fileOffset
				valueBuffer = new byte[8];								// Re-initialize the byte buffer - physical maximum (8 bytes), e.g. 500 or 40
				edfFileReader.seek(fileOffset + (channelId * 8));		// Position the file pointer to appropriate channel-specific location for attribute "physical minimum"
				edfFileReader.readFully(valueBuffer);					// Read in the physical maximum
				channelSpecificMetadata.put("Physical Maximum", 		// Add the physical maximum to the hashtable
						new String(valueBuffer).trim());
				
				fileOffset = fileOffset + (numChannels * 8);			// Increment the fileOffset
				valueBuffer = new byte[8];  							// Re-initialize the byte buffer (8 bytes), e.g. -2048
				edfFileReader.seek(fileOffset + (channelId * 8));		// Position the file pointer to appropriate channel-specific location for attribute "digital minimum" 
				edfFileReader.readFully(valueBuffer);					// Read in the digital minimum
				channelSpecificMetadata.put("Digital Minimum", 			// Add the digital minimum to the hashtable
						new String(valueBuffer).trim());
				
				fileOffset = fileOffset + (numChannels * 8);			// Increment the fileOffset 
				valueBuffer = new byte[8];								// Re-initialize the byte buffer (8 bytes), e.g. 2047
				edfFileReader.seek(fileOffset + (channelId * 8));		// Position the file pointer to appropriate channel-specific location for attribute "digital maximum"
				edfFileReader.readFully(valueBuffer);					// Read in the digital maximum
				channelSpecificMetadata.put("Digital Maximum", 			// Add the digital maximum to the hashtable
						new String(valueBuffer).trim());
				
				fileOffset = fileOffset + (numChannels * 8);			// Increment the fileOffset
				valueBuffer = new byte[80];								// Re-initialize the byte buffer (80 bytes), e.g. HP:0.1Hz LP:75Hz 
				edfFileReader.seek(fileOffset + (channelId * 80));		// Position the file pointer to the appropriate channel-specific location for attribute "prefiltering"
				edfFileReader.readFully(valueBuffer);					// Read in the prefiltering value
				channelSpecificMetadata.put("Prefiltering", 			// Add the prefiltering to the hashtable
						new String(valueBuffer).trim());
				
				fileOffset = fileOffset + (numChannels * 80);			// Increment the fileOffset
				valueBuffer = new byte[8];								// Re-initialize the byte buffer (8 bytes)
				edfFileReader.seek(fileOffset + (channelId * 8));		// Position the file pointer to the appropriate channel-specific location for attribute "number of samples in each data record" 
				edfFileReader.readFully(valueBuffer);					// Read in the number of samples per data record
				channelSpecificMetadata.put("Samples per Data Record", 	// Add the number of samples per data record value to the hashtable
						new String(valueBuffer).trim());
				
				fileOffset = fileOffset + (numChannels * 8);			// Increment the fileOffset
				valueBuffer = new byte[32];								// Re-initialize the byte buffer (32 bytes)
				edfFileReader.seek(fileOffset + (channelId * 32));		// Position the file pointer to the appropriate channel-specific location for attribute "reserved"
				edfFileReader.readFully(valueBuffer);					// Read in the reserved value
				channelSpecificMetadata.put("Reserved", 				// Add the reserved value to the hashtable
						new String(valueBuffer).trim());
				
				channelMetadata.add(channelId, channelSpecificMetadata);// Add the channel-specific metadata to the arraylist for all channel metadata - a hastable of values
			}														// ENDOF FOR loop over each channel
			edfFileReader.close();									// Close the file reader
		} catch(IOException e){
			System.out.println("IOException: ");
			e.printStackTrace();
		}														// ENDOF Try/Catch Block for Reading File
		return channelMetadata;									// RETURN the populated list of hashtables
	}
	// ENDOF EDFChannelMetadataExtractor Method Definition
	
}
