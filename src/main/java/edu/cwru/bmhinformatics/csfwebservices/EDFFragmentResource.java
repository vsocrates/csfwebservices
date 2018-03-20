package edu.cwru.bmhinformatics.csfwebservices;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.bind.annotation.JsonbVisibility;
import javax.json.stream.JsonGenerator;
import javax.json.stream.JsonGeneratorFactory;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

import edu.cwru.bmhinformatics.csfwebservices.EDFSegment;

@Path("fragment")
public class EDFFragmentResource {

	private static final int[] DAYS_IN_MONTH = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

	
	/**
	 * 
	 * RESTful Resource methods
	 * 
	 * will be passed epoch, channel metadata, and study metadata
	 * 
	 */

	@POST
	@Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
	@Path("count")
	public String createFragments(String data) {
		
		JsonReader jsonParamsReader = Json.createReader(new StringReader(data));
		JsonObject inputParams = jsonParamsReader.readObject();

		String edfFileDir = inputParams.getString("edfFileDir");
//		String newCSFDir = inputParams.getString("newCSFDir");
		
		String studyMetadataDir = inputParams.getString("metadataDir");
		int incrementNumberParam = inputParams.getInt("numEpochsPerSegment");
		
		File edfFilesDir = new File(edfFileDir);
		
		
		int epochDuration = inputParams.getInt("epoch");
				
		for(File edfFile: edfFilesDir.listFiles()) {
			if (edfFile.getPath().indexOf(".edf") == -1) continue;								// Skip the .txt file and process only .edf files
			
			String edfFileName = edfFile.getName().substring(0, edfFile.getName().length() - 4);
			String studyMetadataFullPath = studyMetadataDir + "/" + edfFileName + "_studymetadata.json";
			String channelMetadataFullPath = studyMetadataDir + "/" + edfFileName + "_channelmetadata.json";
			String annotationsFullPath = studyMetadataDir + "/" + edfFileName + "_annotations.json";
			
			System.out.println("annoation: " + edfFile.getAbsolutePath());
			InputStream fis, fis2, fis3;
			JsonObject studyMetadataJson = null;
			JsonArray channelMetadataJson = null;
			JsonObject annotationJson = null;

			try {
				fis = new FileInputStream(studyMetadataFullPath);
				JsonReader studyMetadataReader = Json.createReader(fis);
				studyMetadataJson = studyMetadataReader.readObject();
				studyMetadataReader.close();

				fis2 = new FileInputStream(channelMetadataFullPath);
				JsonReader channelMetadataReader = Json.createReader(fis2);
				channelMetadataJson = channelMetadataReader.readArray();
				channelMetadataReader.close();

				fis3 = new FileInputStream(annotationsFullPath);
				JsonReader annotationReader = Json.createReader(fis3);
				annotationJson = annotationReader.readObject();
				annotationReader.close();

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

			if (epochDuration <= 0)
				epochDuration = 30;

			//the annotation file used to be part of the studymetadata, but now its not, so we need to extract it separately
			LinkedHashMap<String,String> annotations = SDAuxiliary.jsonToHashmap(annotationJson);
			LinkedHashMap <String, String> studyMetadata = SDAuxiliary.jsonToHashmap(studyMetadataJson);
			ArrayList<LinkedHashMap<String, String>> channelMetadata = SDAuxiliary.jsonToHashmapList(channelMetadataJson);
			String fragmentDetails 															// Retrieve the total number of fragments and size of each fragment 
			= EDFSignalFragmentCount(		 												//	(in bytes) corresponding to specified time duration (epoch, in seconds)
					studyMetadata, channelMetadata, epochDuration);			

			int numberofFragments = Integer.parseInt(fragmentDetails.split(",")[0]);		// Parse out the total number of fragments and size of each fragment from string (comma separated values)
			System.out.println("test number: " + Integer.toString(numberofFragments));
			int sizeofFragment = Integer.parseInt(fragmentDetails.split(",")[1]);			// Parse out the size of each fragment 
			System.out.println("Number of fragments, size of each fragment (bytes): "
					+ numberofFragments + ", " + sizeofFragment);
			int incrementNumber = incrementNumberParam;								// Get the number of epochs to place in each segment from user input

			for(int i = 0; i < numberofFragments; i += incrementNumber) {					// In a for loop, retrieve specific number of fragments from the EDF file
				System.out.println("Fragment counter: " + i + "\n");
				EDFSegment edfSegmentObject 													// Check to retrieve only available number of fragments 
				= EDFSignalFragments(edfFile, studyMetadata, 			// - last set of fragments may be less than increment number
						channelMetadata, annotations, epochDuration, i, 
						(numberofFragments - i < incrementNumber) ? numberofFragments : i + incrementNumber - 1);

				edfSegmentObject = ExtractChannelData(edfSegmentObject);			// Process each EDF Data Record EDFFragment to create channel-specific signal data fragments 
				// - replace EDF Data Record with channel-oriented signal data fragments
				// edfSegmentObject.getAnnotationList();										// Retrieve the list of annotations
				// signalByteValue = signalDataFragments.getSignalSpecificData();				// Get the signal data from the EDFFragment object
				edfSegmentObject = channelSignalBinaryToPhysical(edfSegmentObject);				// Convert the signal data from byte to short (EDF stores signal data in 16-bit integers in 2's complement)
				// System.out.println("Path: " + args[0] + "/CSF/" + (file_array[file_num].getName()).replace(".edf", ("_part" + edfSegmentObject.firstFragmentSequence + ".json")) + "\n");	        		

				try {
					String time = CloudwaveSignalFormatGenerator(true, edfSegmentObject, 	// Generate CSF json files for each EDFSegmentObject
							edfFileDir, edfFile.getName());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				String CSFfile = edfFile.getName().replace(".edf", 		
						("_part" + edfSegmentObject.getFirstFragmentSequence() + ".json"));
				//System.out.println("End i: " + i + "\n");        	
				//				CSFLocations.add(time + ": " + CSFfile);
				// CSFIndex.put(time, Integer.toString(CSFLocations.size() - 1));
			}																				// End of for loop - retrieving available fragments
			
		}
		
		return "Broke down fragments";

	}

	/**
	 * 
	 * Auxiliary methods
	 * 
	 */

	/** 
	 * @param: EDF file, study metadata and clinical annotations, channel-specific metadata, epoch size (in seconds)
	 * Count the total number of fragments and size of each fragment (in bytes) for given epoch size
	 * @result: A two-value string (separated by comma) <number of fragments, size of each fragment (in bytes)>
	 * 
	 */
	public static String EDFSignalFragmentCount( LinkedHashMap <String, String> studyMetadata,  
			ArrayList <LinkedHashMap <String, String>> channelMetadata, 
			double epochDuration) {		

		System.out.println("studym eta: " +studyMetadata.get("Data Records Duration"));
		double durationDDR = Double.parseDouble(studyMetadata.get("Data Records Duration"));	// Retrieve the duration of data record (in seconds)
		int numberDRE = (int) Math.ceil(epochDuration / durationDDR);							// Calculate the number of data records per epoch 
		// 	use the java Math class ceil method to get "the smallest (closest to negative infinity) double value that is greater than or equal to the argument and is equal to a mathematical integer."
		int totalDR = Integer.parseInt(studyMetadata.get("Number of Data Records"));			// Retrieve the total number of data records
		int numberFragments = ((int) (totalDR / numberDRE)) 
				+ ((totalDR % numberDRE == 0) ? 0 : 1);											// Calculate the total number of fragments (TF)

		/**
		 * Divide the signal data in EDF file into fragments where each fragment is a byte array. 
		 * Size of fragment (in bytes) (SF) = 2 (size of each sample in bytes) * total bytes per data record (BDR) * number of data records per fragment (or epoch since by default 1 fragment = 1 epoch)
		 * Number of bytes per record (BDR) = Aggregate of (number of samples per channel) for total number of channels
		 * 
		 */
		int numChannels = Integer.parseInt(studyMetadata.get("Number of Signals"));				// Retrieve the total number of channels
		int numSamples = 0;																		// Initialize variable to store number of samples
		for(int counter = 0; counter < numChannels; counter++)									// In a for loop, aggregate number of samples per signal channel
			numSamples += Integer.parseInt(channelMetadata.get(counter)
					.get("Samples per Data Record").toString()); 		// Retrieve number of samples for given channel

		int bytesperDR = numSamples * 2;														// Calculate the total bytes per data record (BDR)
		int bytesperFragment = bytesperDR * numberDRE;											// Calculate the total bytes per fragment (equals 1 epoch by default) 
		// 	= number of bytes per data record * number of data records per epoch
		return numberFragments + "," + bytesperFragment;										// RETURN a string value consisting of total number of fragments and size 
		// 	of each fragment (in bytes) separated by comma
	}

	/** 
	 * Extract and segment signal data into fragments based on size of epochs
	 * @param: EDF file, study-specific metadata, channel-specific metadata, epoch size (in seconds)
	 * @result: ArrayList of fragments corresponding to size of epochs (in seconds)
	 * 
	 */
	public static EDFSegment EDFSignalFragments(File edfFile, 
			LinkedHashMap <String, String> signalMetadata, 
			ArrayList <LinkedHashMap <String, String>> channelMetadata, 
			LinkedHashMap <String, String> annotations,
			double epochDuration, int startFragment, int endFragment){
		ArrayList <byte[]> signalDataFragment = new ArrayList <byte[]> ();
		LinkedHashMap <String, String> studyMetadata = signalMetadata;						// Retrieve the study-specific metadata hashtable		
		try {					    															// BEGIN Try/Catch Block to Read File
			/** 
			 * The total number of fragments (TF) = (Total number of data records (TDR)/ Number of Data Records per Epoch (DRE)) + 1 (Fragment),
			 * where Size of Epoch (in seconds) (SE) = DRE * Duration of Data Records (in seconds) (DDR)
			 * => DRE = SE (in seconds)/DDR (in seconds)
			 * 1 Fragment to store remainder of TDR/DRE
			 * 
			 * Each fragment can hold data records corresponding to 1 or more epoch (in seconds). Default is 1 epoch per fragment  
			 * 
			 */
			double durationDDR = Double.parseDouble(studyMetadata.get("Data Records Duration"));	// Retrieve the duration of data record (in seconds)
			int numberDRE = (int) Math.ceil(epochDuration /durationDDR);							// Calculate the number of data records per epoch 
			//	use the Math.ceil method to get "the smallest (closest to negative infinity) double value that is greater than or equal to the argument and is equal to a mathematical integer."
			int totalDR = Integer.parseInt(studyMetadata.get("Number of Data Records"));			// Retrieve the total number of data records
			int numberFragments = (int) (totalDR / numberDRE);										// Calculate the total number of fragments (TF)
			int lastFragmentDR = totalDR % numberDRE;												// Get the remainder value corresponding to number of records left over after TF fragments created

			/**
			 * Divide the signal data in EDF file into fragments where each fragment is a byte array. 
			 * Size of fragment (in bytes) (SF) = 2 (size of each sample in bytes) * totalbytes per data record (BDR) * number of data records per fragment (or epoch since by default 1 fragment = 1 epoch)
			 * Number of bytes per record (BDR) = Aggregate of (number of samples per channel) for total number of channels
			 * 
			 */
			int numChannels = Integer.parseInt(studyMetadata.get("Number of Signals"));				// Retrieve the total number of channels
			int numSamples = 0;																		// Initialize variable to store number of samples
			for(int counter = 0; counter < numChannels; counter++) 									// In a for loop, aggregate number of samples per signal channel
				numSamples += Integer.parseInt(channelMetadata.get(counter)								// Retrieve number of samples for given channel
						.get("Samples per Data Record").toString());
			int bytesperDR = numSamples * 2;														// Calculate the total bytes per data record (BDR)
			int bytesperFragment = bytesperDR * numberDRE;											// Calculate the total bytes per fragment (equals 1 epoch by default) 
			// 	= bytes per data record * number of data records per fragment

			RandomAccessFile edfFileReader = new RandomAccessFile(edfFile, "r");										// Initialize the random access file reader
			int headerOffset = Integer.parseInt(studyMetadata.get("Number of Header Bytes"));		// Get the total size of header (in bytes)
			edfFileReader.seek(headerOffset + (startFragment * bytesperFragment));					// Position the file pointer to start of the signal data 
			// 	offset by size of header bytes and the fragment number
			/**
			 * The method returns specific number of fragments - two counters are specified
			 * the start and end count are used to loop through the file
			 * 
			 */
			for(int counter = startFragment; counter < endFragment + 1; counter++) {				// In a for loop, read in the bytes per each fragment and add to an arraylist
				byte[] valueBuffer = new byte[Math.min(bytesperFragment, 								// Initialize the byte buffer to store the signal data according to number of bytes left in file to read
						(int) (edfFileReader.length() - edfFileReader.getFilePointer()))];				// 	The file length and file offset position methods on RandomFileAccess return long, converted to int
				edfFileReader.readFully(valueBuffer);													// Read in the bytes corresponding to 1 fragment
				signalDataFragment.add(valueBuffer);													// Add the byte array to the array list
				// bytesperEDFFragment += valueBuffer.length;											// Aggregate total number of bytes for signal data in EDFFragment object
			}																						// ENDOF For Loop Reading Bytes per Fragment		    

			if((lastFragmentDR > 0) && ((endFragment + 1) == numberFragments)) {					// IF there are left over data records (remainder from TDR/DRE) and if the endFragment value equals the total number of fragments (last fragment)
				byte[] valueBuffer = new byte[bytesperDR * lastFragmentDR];								// Initialize the byte array corresponding to number of data records in remainder
				edfFileReader.readFully(valueBuffer);													// Read in the specified number of bytes from file
				signalDataFragment.add(valueBuffer);													// Add the last fragment to the array list
			}																						// ENDIF

			edfFileReader.close();		    														// Close the random file access reader
		} catch(IOException e) {
			System.out.println("IOException: ");
			e.printStackTrace();
		}																						// ENDOF Try/Catch Block to Read File

		/**
		 * Add the clinical event annotation, study metadata, channel specific metadata, and signal data to EDFFragment object with sequence number of the fragments
		 * 
		 */
		return new EDFSegment(signalMetadata, channelMetadata, annotations, signalDataFragment, 				// RETURN the populated List of signal data segments
				startFragment, endFragment, epochDuration);      

	}

	/**
	 * Method to extract channel-specific signal data from EDFFragments
	 * @param: EDFSegment with array of signal data in byte format
	 * @return: EDFSegment with a two-dimensional arraylist - signal data per fragment and per channel
	 * 
	 */
	public static EDFSegment ExtractChannelData(EDFSegment inputSegment){		
		LinkedHashMap <String, String> studyMetadata = inputSegment.getStudyMetadata();			// Get the hashtable object with study-specific metadata
		ArrayList <LinkedHashMap <String, String>> channelSpecificMetadata 
		= inputSegment.getChannelMetadata();											// Get the arraylist object for channel-specific metadata
		int numChannel = Integer.parseInt(studyMetadata.get("Number of Signals")); 				// Retrieve the count for total number of channels in each data record

		/** 
		 * Compute the number of bytes per data record
		 * 
		 */
		int numSamples = 0;																		// Initialize the number of samples variable
		for(int counter1 = 0; counter1 < numChannel; counter1++)								// In a for loop, aggregate number of samples per signal channel
			numSamples += Integer.parseInt(channelSpecificMetadata.get(counter1)					// Retrieve number of samples for given channel
					.get("Samples per Data Record").toString());
		int bytesPerRecord = numSamples * 2;													// Compute the total number of bytes per data record

		/**
		 * Memory profiling
		 * 
		 */
		Runtime.getRuntime().gc();																// Get the current Java runtime and run the garbage collector	    

		/**
		 * In a nested for loop:
		 * a) For total number of signal fragment (each fragment equals 1 epoch by default) in EDFSegment object - outer for loop
		 * b) For total number of data records in each signal fragment, 
		 * c) For each channel, retrieve and aggregate signal data for each channel
		 * 
		 */    
		ArrayList <ArrayList<byte[]>> signalDataFragment = new ArrayList <ArrayList<byte[]>> ();
		for(int counter1 = 0; counter1 < inputSegment.getSignalData().size(); counter1++) {		// For each signal fragment
			/**
			 * The ArrayList of signal data in EDFSegment object has multiple fragments (each equals to 1 epoch by default). 
			 * 1 fragment contains multiple data records. Retrieve the total number of bytes per fragment -
			 * each fragment is of equal length, except last fragment that may have less number of bytes. 
			 *  
			 */
			int signalFragmentSize = inputSegment.getSignalData().get(counter1).length;
			int numDRE = (int) (signalFragmentSize / bytesPerRecord);								// Get total number of data records in each fragment (equal to 1 epoch by default)
			ByteBuffer bBuffer = ByteBuffer.allocate(inputSegment.getSignalData()					// Allocate appropriate size to ByteBuffer
					.get(counter1).length);
			bBuffer = ByteBuffer.wrap(inputSegment.getSignalData().get(counter1));					// Wrap a ByteBuffer aroung the buffer
			ArrayList <byte[]> channelDataFragment = new ArrayList <byte[]>();						// Initialize the arraylist to hold the signal data for given fragment
			// channelDataFragment = new Hashtable <String, ArrayList <byte[]>>();
			for(int counter2 = 0; counter2 < numDRE; counter2++) {									// For each data record in the fragment (contains 1 or more epoch)
				for(int counter3 = 0; counter3 < numChannel; counter3++) {								// For each channel in 1 data record
					numSamples = Integer.parseInt(channelSpecificMetadata.get(counter3)						// Retrieve number of samples for given channel
							.get("Samples per Data Record").toString());
					byte[] dataBuffer = new byte[numSamples * 2];											// Initialize the byte buffer to copy the data for this channel
					bBuffer.get(dataBuffer, 0, dataBuffer.length);											// Read in the specific number of bytes per channel into temporary buffer 
					// 	the ByteBuffer get() method advances the read position by "length" of the destination buffer
					/** 
					 * Retrieve the channel-specific byte buffer from arraylist channelSignal
					 * Use arrayCopy to concatenate the two byte arrays
					 */
					if(counter2 > 0) {																		// IF this is NOT the first Data Record for given fragment
						byte[] channelBuffer 																	// Initialize the byte array to store existing and new channel data
						= new byte[dataBuffer.length + channelDataFragment.get(counter3).length];
						System.arraycopy(channelDataFragment.get(counter3), 0, 									// Retrieve the channel-specific byte array from the arraylist and copy to new byte array
								channelBuffer, 0, channelDataFragment.get(counter3).length);
						System.arraycopy(dataBuffer, 0, 														// Copy new signal data to new byte array
								channelBuffer, channelDataFragment.get(counter3).length, dataBuffer.length);	
						channelDataFragment.set(counter3, channelBuffer);
					} else {																				// ELSE there is no previous channel specific data
						byte[] channelBuffer = new byte[dataBuffer.length];										// Initialize the byte array to store existing and new channel data
						System.arraycopy(dataBuffer, 0, channelBuffer, 0, dataBuffer.length);					// Copy new signal data to new byte array
						channelDataFragment.add(counter3, channelBuffer);
					}																						// ENDIF			    			
				}																						// ENDOF Inner For-Loop for Channels
			}																						// ENDOF Outer For-Loop for Data Records
			signalDataFragment.add(channelDataFragment);											// Add the arraylist with byte arrays corresponding to channel-specific data
		}																						// ENDOF For-Loop over all fragments in EDFSegment object
		inputSegment.putChannelOrientedData(signalDataFragment);								// Put the two-dimensional array to the EDFSegment object to replace original two-dimensional array
		return inputSegment;																	// RETURN the updated EDFSegment object
		// ALG: Why is a return necessary? Is EDFSegment immutable?
	}

	/**
	 * Method to convert signal data in binary format to short integer format 
	 * @param two-dimensional inputArray: an ArrayList of ArrayList of byte[] with signal data in byte format
	 * @return two-dimensional resultArray an ArrayList of byte[] with signal data in short format
	 * 
	 */		
	private static EDFSegment channelSignalBinaryToPhysical(EDFSegment edfSegmentObject) {
		ArrayList <ArrayList <double[]>> resultArray_file = new ArrayList <ArrayList <double[]>>();
		float phyMin = Float.parseFloat(edfSegmentObject.getChannelMetadata().get(0).get("Physical Minimum").toString());
		float phyMax = Float.parseFloat(edfSegmentObject.getChannelMetadata().get(0).get("Physical Maximum").toString());
		int digMin = Integer.parseInt(edfSegmentObject.getChannelMetadata().get(0).get("Digital Minimum").toString());
		int digMax = Integer.parseInt(edfSegmentObject.getChannelMetadata().get(0).get("Digital Maximum").toString());

		// valueBuffer_file= edfSegmentObject.edfSignalSpecificDataFragment;
		ArrayList <ArrayList <byte[]>> valueBuffer_file 								// Read binary raw channel-specific signal data from EDFSegment
		= edfSegmentObject.getChannelSpecificData();		
		System.out.println("Fragment num:" + valueBuffer_file.size());

		for(int counter = 0; counter < valueBuffer_file.size(); counter++) {			// In a for loop, extract DataFragments of signal data from input array and convert to short value
			ArrayList <byte[]> valueBuffer_DataFragment = valueBuffer_file.get(counter);	// get DataFragment for whole dataFragment
			ArrayList <double[]> resultArray_DataFragment = new ArrayList <double[]>();
			for(int counter_seg = 0; 
					counter_seg < valueBuffer_DataFragment.size(); counter_seg++) {			// In a for loop, extract segments of signal data from input array and convert to int value
				byte[] valueBuffer = valueBuffer_DataFragment.get(counter_seg);					// Copy the signal fragment to the byte [] array
				int buffSize = (valueBuffer.length / 2) 										// Initialize the size object for ByteBuffer and ShortBuffer 
						+ ((valueBuffer.length % 2 == 0) ? 0 : 1);								// - rounding off the size of buffer
				short[] resultBuffer = new short[buffSize];										// Initialize the short array
				int index = 0;
				for(int i = 0; i < valueBuffer.length; i += 2) {
					short s0 = (short) (valueBuffer[i] & 0xff);
					short s1 = (short) (valueBuffer[i + 1] & 0xff);
					s1 <<= 8;
					// short s = (short) (s0 | s1);
					resultBuffer[index] = (short) (s0 | s1); // s;
					index++;
				}

				/** 
				 * In a for loop,
				 * physicalValue = [(physical minimum) + (digital value in the data record - digital minimum) 
				 * 		* (physical maximum - physical minimum) / (digital maximum - digital minimum)]
				 * 
				 */
				double[] phyArray = new double[buffSize];
				for(int d = 0; d < buffSize; d++) 
					phyArray[d] = phyMin 
					+ (resultBuffer[d] - digMin) * (phyMax - phyMin) / (digMax - digMin);
				resultArray_DataFragment.add(phyArray);											// Add the result to arraylist
			}																				// End of for - convert data in a segment to int
			resultArray_file.add(resultArray_DataFragment);									// Add the result to arraylist
		}																				// End of for - extract fragments (consisting of multiple segments) and convert to short
		edfSegmentObject.setChannelSpecificDataDouble(resultArray_file);				// Assign the converted values to member variable of edfSegmentObject
		return edfSegmentObject;
	}

	/**
	 * Create a JSON object corresponding to given input EDF data
	 * @param: EDFSegment edfSegmentObject: the given EDF data
	 * @param: String csfDir: the directory in which to place the JSON file
	 * @param: String filename: the file destination for the JSON Object
	 * @return: String containing the start date and time of the Cloudwave Signal Format JSON Object
	 * @throws IOException 
	 * 
	 */
	public static String CloudwaveSignalFormatGenerator(boolean prettyPrint, EDFSegment edfSegmentObject, String csfDir, String filename) throws IOException {
		
		HashMap<String, Boolean> config = new HashMap <String, Boolean>();
		JsonBuilderFactory jsonFactory = Json.createBuilderFactory(null);
		

		
		config.put(JsonGenerator.PRETTY_PRINTING, prettyPrint);
		JsonGeneratorFactory CSFFactory = Json.createGeneratorFactory(config);
		LinkedHashMap <String, String> edfStudyMetadata = edfSegmentObject.getStudyMetadata();
		ArrayList <LinkedHashMap <String, String>> edfChannelMetadata = edfSegmentObject.getChannelMetadata();
		LinkedHashMap <String, String> annotationList = edfSegmentObject.getAnnotationList();
		
		File outputJSONFile 
				= new File(csfDir + "/" + filename.replace(".edf", "-CSF/"));			// Create a directory to store JSON 
		outputJSONFile.mkdir();															// Make the directory
		outputJSONFile = new File(csfDir + "/" + filename.replace(".edf", "-CSF/") 		// Naming scheme for CSF files - original file name + "part" + fragmentsequence
				+ filename.replace(".edf", ("_part" + edfSegmentObject.getFirstFragmentSequence() + ".json")));
		System.out.println("JSON file: " + outputJSONFile.toString() + "\n");
		
		JsonGenerator CSFGenerator 														// Initialize the output stream to write out the JSON file
			 = CSFFactory.createGenerator(new BufferedWriter(new FileWriter(outputJSONFile)));
		CSFGenerator.writeStartObject();												// Start writing the JSON file
		
		/**
		 * Create Jsonobject metadata of the segment
		 * Start and end time for each of the fragments stored in this segment
		 * 
		 */
		String edfStartDate 
				= edfStudyMetadata.get("Recording Start Date").toString();				// Retrieve the start date of the signal data for the entire recording in EDF file
		String edfStartTime 
				= edfStudyMetadata.get("Recording Start Time").toString();				// Retrieve the start time of the signal data for the entire recording in EDF file
		System.out.println(edfStartDate + " " + edfStartTime);
		
		String[] dateArray = edfStartDate.split("\\.|:");									// Split the start date values using "." as separator
		String[] timeArray = edfStartTime.split("\\.|:");									// Split the start time values using "." as separator
				
		int firstFragment = edfSegmentObject.getFirstFragmentSequence();				// Retrieve the fragment sequence numbers
		int lastFragment = edfSegmentObject.getLastFragmentSequence();
		System.out.println("first fragment: " + firstFragment + ", "
				+ "last fragment: "+ lastFragment);

		JsonObjectBuilder dateTimeBuilder = jsonFactory.createObjectBuilder();			// Initialize the JsonObjectBuilder for the date/time values of each fragment
		// int numFragments = 1 + lastFragment - firstFragment;
		dateTimeBuilder.add("FirstFragment", firstFragment);
		dateTimeBuilder.add("LastFragment", lastFragment);
		double epoch = edfSegmentObject.getEpochDuration();
		dateTimeBuilder.add("EpochDuration", epoch);
		String SegmentStartDate = "", SegmentStartTime = "";
		for(int i = firstFragment; i < (lastFragment + 1); i++) {						// FOREACH fragment, compute the start and end recording time of the fragments in the EDFSegment
			double currentStartTime = epoch * i;											// Compute the total time offset (in seconds) from initial recording time to recording start time for current fragment
			//System.out.println("Total seconds: " + currentStartTime + "\n");

			/**
			 * Transform the total offset time (in seconds) to appropriate values in seconds, minutes, hour, and days
			 */
			int offsetSecond = ((int) currentStartTime) % 60;								// Calculate the offset seconds
			double numMinutes = (currentStartTime - offsetSecond) / 60;						// Compute total minutes
			int offsetMinute = ((int) numMinutes) % 60;										// Compute the offset minutes
			double numHours = (numMinutes - offsetMinute) / 60;								// Compute total hours
			int offsetHour = ((int) numHours) % 24;											// Compute the offset hours
			int numDays = (int) ((numHours - offsetHour) / 24);
			// int offsetDay = ((int) timeVar) % 24;										// Compute the total number of days
			// System.out.println("Complete time: seconds - " + offsetSecond 
			//		+ ", mins - " + offsetMinute + ", hour - " + offsetHour
			//		+ ", day - " + offsetDay + "\n");			
			
			offsetSecond += Integer.parseInt(timeArray[2]);									// Add seconds offset to the start time of the current fragment
			offsetMinute += Integer.parseInt(timeArray[1]);									// Add minutes offset to the start time of the current fragment
			offsetHour += Integer.parseInt(timeArray[0]);									// Add hours offset to the start time of the current fragment
			edfStartTime 																	// Concatenate the values in the variable edfStartTime
					= ((offsetHour < 10) ? "0" : "") + Integer.toString(offsetHour)
					+ ((offsetMinute < 10) ? ".0" : ".") + Integer.toString(offsetMinute) 
					+ ((offsetSecond < 10) ? ".0" : ".") + Integer.toString(offsetSecond);
			// System.out.println("StartDate: " 
			//		+ strArr1[0] + strArr1[1] + strArr1[2] + "\n");
								
			// if(offsetDay > 0) calendarVar.add(Calendar.DATE, offsetDay);					// Add the additional day(s) to start of recording for the current fragment
			if(numDays > 0) {
				int D = Integer.parseInt(dateArray[0]), 
					M = Integer.parseInt(dateArray[1]), 
					Y = Integer.parseInt(dateArray[2]);
				while(D > DAYS_IN_MONTH[M - 1] 
						+ ((M == 2 && (Y % 4 == 0 && (Y % 100 != 0 || Y % 400 == 0))) ? 1 : 0)) {
					D -= DAYS_IN_MONTH[M++ - 1];
					if(M > 12) {M = 1; Y++;}
				}
				Y %= 100;
			edfStartDate = ((D < 10) ? "0" : "") + Integer.toString(D)						// Concatenate the values in the variable edfStartTime
							+ ((M < 10) ? ".0" : ".") + Integer.toString(M) 
							+ ((Y < 10) ? ".0" : ".") + Integer.toString(Y % 100);
			}
			if(i == firstFragment) {SegmentStartDate = edfStartDate; SegmentStartTime = edfStartTime;}
			JsonObjectBuilder dateTimeFragmentBuilder
					= jsonFactory.createObjectBuilder();
			dateTimeFragmentBuilder
					.add("fragmentSequence", i)
					.add("startDate", edfStartDate)
					.add("startTime", edfStartTime);
			// System.out.println(edfStartDate + "," + edfStartTime);
			dateTimeBuilder.add("fragmentSeq" + i, 	dateTimeFragmentBuilder);				// Add the start time to Jsonobject						
		}																				// ENDOF FOR loop computing timestamps for each fragment in this segment
		CSFGenerator.write("Header", dateTimeBuilder.build());							// Build the DateTime JsonObject and write to the file
			
		/**
		 * Create Jsonobject for the study metadata
		 * Assumption: The State Date Format using YY for year - may need to be modified in future
		 * 
		 */
		JsonObjectBuilder studyMetadataJsonObjectBuilder
				= jsonFactory.createObjectBuilder();
		studyMetadataJsonObjectBuilder
				.add("edfFileName", edfStudyMetadata.get("FileName"))
				.add("dataFormatVersion", edfStudyMetadata.get("Data Format Version"))
				.add("localPatientIdentification", edfStudyMetadata.get("Local Patient Identification"))
				.add("recordingStartDate", edfStudyMetadata.get("Recording Start Date"))
				.add("recordingStartDateFormat", "dd.mm.yy")
				.add("recordingStartTime", edfStudyMetadata.get("Recording Start Time"))
				.add("recordingStartTimeFormat", "hh.mm.ss")
				.add("numberHeaderBytes", edfStudyMetadata.get("Number of Header Bytes"))
				.add("numberDataRecords", edfStudyMetadata.get("Number of Data Records"))
				.add("dataRecordDuration", edfStudyMetadata.get("Data Records Duration"))
				.add("dataRecordDurationUnit", "seconds")
				.add("numberSignals", edfStudyMetadata.get("Number of Signals"));
		CSFGenerator.write("StudyMetadata", studyMetadataJsonObjectBuilder.build());
		
		/**
		 * Create Jsonobject for the clinical event annotations
		 * Assumption: The clinical annotations are stored by sequential order: <sequence, "timestamp; event">
		 * 
		 */
		JsonObjectBuilder clinicalAnnotationBuilder 									// Initialize the JsonObjectBuilder to store the clinical annotations in a for loop
				= jsonFactory.createObjectBuilder();
		for(String iteratorKey: annotationList.keySet()) {								// FOREACH time stamp in the annotationList HashMap 
			clinicalAnnotationBuilder														// Extract the annotations and populate the JsonObjectBuilder
					.add(iteratorKey, annotationList.get(iteratorKey).trim()); 
			// System.out.println("Current key: " + annotationTime 
			// 		+ ", value:" + annotationList.get(annotationTime));
		}																				// ENDOF For loop over annotationList -- added the annotation list
		CSFGenerator.write("ClinicalAnnotationList", clinicalAnnotationBuilder.build());
		
		/**
		 * Create Jsonobject for the channel-specific metadata 
		 */
		JsonObjectBuilder channelMetadataBuilder 										// Initialize the Jsonobjectbuilder to store channel-specific metadata
				= jsonFactory.createObjectBuilder();
		String[] channelList = new String[edfChannelMetadata.size()];
		for(int i = 0; i < edfChannelMetadata.size(); i++) {							// FOREACH channel: extract and generate corresponding JSON objects for channel-specific metadata 
			LinkedHashMap <String, String> metadata = edfChannelMetadata.get(i);
			String label = metadata.get("Label");
			channelList[i] = label;
			channelMetadataBuilder
					.add(label, 
						jsonFactory.createObjectBuilder()
									//.add("channelLabel", label)
									.add("channelNumber", Integer.toString(i))
									.add("channelTransducerType", metadata.get("Transducer Type").toString())
									.add("channelPhysicalDimension", metadata.get("Physical Dimension").toString())
									.add("channelPhysicalMinimum", metadata.get("Physical Minimum").toString())
									.add("channelPhysicalMaximum", metadata.get("Physical Maximum").toString())
									.add("channelDigitalMinimum", metadata.get("Digital Minimum").toString())
									.add("channelDigitalMaximum", metadata.get("Digital Maximum").toString())
									.add("channelPrefiltering", metadata.get("Prefiltering").toString())
									.add("channelSamplesPerDataRecord", metadata.get("Samples per Data Record").toString())
									.add("channelReserved", metadata.get("Reserved").toString()));
		}																				// End of for - build JSON objects for channel-specific metadata 
		channelMetadataBuilder.add("channelList", Arrays.toString(channelList));
		CSFGenerator.write("ChannelMetadata", channelMetadataBuilder.build());
		
		/**
		 * Add the signal data corresponding to each fragment to the file. The signal data are stored as  
		 *
		 */
		JsonObjectBuilder dataRecordBuilder = jsonFactory.createObjectBuilder();
		ArrayList <ArrayList <double[]>> fragmentChannelDataList 						// Retrieve data for all fragments
			= edfSegmentObject.getChannelSpecificDataDouble();
		for(int i = 0; i < fragmentChannelDataList.size(); i++) { 						// FOREACH Fragment: Copy over the fragment-specific data to JsonArray
			ArrayList <double[]> channelData = fragmentChannelDataList.get(i);				// Retrieve data for all channels in a fragment
			JsonObjectBuilder fragmentDataBuilder = jsonFactory.createObjectBuilder();
			for(int j = 0; j < channelData.size(); j++) {						// FOREACH Channel in the fragment: Copy over channel-specific signal data to JsonArray
				fragmentDataBuilder
						.add(edfChannelMetadata.get(j).get("Label").toString(), 
							Arrays.toString(channelData.get(j)));
			}																				// ENDOF For loop over channels
			dataRecordBuilder.add("FragmentNumber" + (firstFragment + i), 
									fragmentDataBuilder);
		}
		CSFGenerator.write("DataRecords", dataRecordBuilder.build());
		
		CSFGenerator.writeEnd();
		CSFGenerator.flush();
		CSFGenerator.close();
		
		return SegmentStartDate + "," + SegmentStartTime;
	}

}
