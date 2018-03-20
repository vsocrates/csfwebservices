package edu.cwru.bmhinformatics.csfwebservices;

import java.io.*;
import java.util.*;

import javax.json.*;
import javax.json.stream.*;

public class SDCSFGenerator {
	
	private static final int[] DAYS_IN_MONTH = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
	
	private static HashMap <String, Boolean> config = new HashMap <>();
	private static JsonBuilderFactory jsonFactory = Json.createBuilderFactory(null);
	
	/**
	 * Create a JSON object corresponding to given input EDF data
	 * @param: Boolean prettyPrint: toggle for pretty printing of csf files
	 * @param: EDFSegment edfSegmentObject: the given EDF data
	 * @param: String csfDir: the directory in which to place the JSON file
	 * @param: String filename: the file destination for the JSON Object
	 * @return: String containing the start date and time of the Cloudwave Signal Format JSON Object
	 * @throws IOException 
	 * 
	 */
	public static String CloudwaveSignalFormatGenerator(boolean prettyPrint, EDFSegment edfSegmentObject, 
														File csfDir, String filename) throws IOException {
		config.put(JsonGenerator.PRETTY_PRINTING, prettyPrint);							// Enable/disable Pretty Printing of CSF files
		JsonGeneratorFactory CSFFactory = Json.createGeneratorFactory(config);			// Create JSON Generator for CSF files
		LinkedHashMap <String, String> edfStudyMetadata 
				= edfSegmentObject.getStudyMetadata();									// Get study metadata from EDFSegment
		ArrayList <LinkedHashMap <String, String>> edfChannelMetadata 
				= edfSegmentObject.getChannelMetadata();								// Get channel metadata from EDFSegment
		LinkedHashMap <String, String> annotationList 
				= edfSegmentObject.getAnnotationList();									// Get list of annotations from EDFSegment
		
		File CSFOutputFile = new File(csfDir, filename + ".csf");
		CSFOutputFile.createNewFile();
		System.out.println("CSF file: " + CSFOutputFile.toString() + "\n");
		
		BufferedWriter csfWriter = new BufferedWriter(new FileWriter(CSFOutputFile));
		JsonGenerator CSFGenerator = CSFFactory.createGenerator(csfWriter);				// Initialize the output stream to write out the JSON file
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
		String edfEndDate 
				= edfStudyMetadata.get("Recording End Date").toString();				// Retrieve the start date of the signal data for the entire recording in EDF file
		String edfEndTime 
				= edfStudyMetadata.get("Recording End Time").toString();				// Retrieve the start time of the signal data for the entire recording in EDF file
		System.out.println(edfEndDate + " " + edfEndTime);
		//int edfDuration 
			//= SDAuxiliary.elapsedSeconds(edfStartDate + "," + edfStartTime, 
					//edfEndDate + "," + edfEndTime);
		
		String[] dateArray = edfStartDate.split("\\.|:");								// Split the start date values using "." as separator
		String[] timeArray = edfStartTime.split("\\.|:");								// Split the start time values using "." as separator
				
		int firstFragment = edfSegmentObject.getFirstFragmentSequence();				// Retrieve the fragment sequence numbers
		int lastFragment = edfSegmentObject.getLastFragmentSequence();
		System.out.println("first fragment: " + firstFragment + ", "
				+ "last fragment: "+ lastFragment);

		JsonObjectBuilder dateTimeBuilder = jsonFactory.createObjectBuilder();			// Initialize the JsonObjectBuilder for the date/time values of each fragment
		dateTimeBuilder.add("FirstFragment", firstFragment);
		dateTimeBuilder.add("LastFragment", lastFragment);
		//double epoch = edfSegmentObject.getEpochDuration();
		//dateTimeBuilder.add("EpochDuration", epoch);
		ArrayList <Double> epochs = edfSegmentObject.getEpochDurations();
		String SegmentStartDate = "", SegmentStartTime = "";
		for(int i = firstFragment; i < (lastFragment + 1); i++) {						// FOREACH fragment, compute the start and end recording time of the fragments in the EDFSegment
			double epoch = epochs.get(i - firstFragment);
			double currentStartTime = epoch * i + Integer.parseInt(timeArray[2]);			// Compute the total time offset (in seconds) from initial recording time to recording start time for current fragment
 
			/**
			 * Transform the total offset time (in seconds) to appropriate values in seconds, minutes, hour, and days
			 */
			int offsetSecond = ((int) currentStartTime) % 60;								// Calculate the offset seconds
			double numMinutes 																// Compute total minutes
					= (currentStartTime - offsetSecond) / 60 + Integer.parseInt(timeArray[1]);
			int offsetMinute = ((int) numMinutes) % 60;										// Compute the offset minutes
			double numHours 
					= (numMinutes - offsetMinute) / 60 + Integer.parseInt(timeArray[0]);	// Compute total hours
			int offsetHour = ((int) numHours) % 24;											// Compute the offset hours
			int numDays = (int) ((numHours - offsetHour) / 24);
			
			edfStartTime 																	// Concatenate the values in the variable edfStartTime
					= ((offsetHour < 10) ? "0" : "") + Integer.toString(offsetHour)
					+ ((offsetMinute < 10) ? ".0" : ".") + Integer.toString(offsetMinute) 
					+ ((offsetSecond < 10) ? ".0" : ".") + Integer.toString(offsetSecond);
			
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
			if(i == firstFragment) {
				SegmentStartDate = edfStartDate;
				SegmentStartTime = edfStartTime;
			}
			JsonObjectBuilder dateTimeFragmentBuilder = jsonFactory.createObjectBuilder();
			dateTimeFragmentBuilder
					.add("FragmentNumber", i)
					.add("startDate", edfStartDate)
					.add("startTime", edfStartTime)
					.add("epoch", epoch);
			dateTimeBuilder.add("FragmentNumber_" + i, 
								dateTimeFragmentBuilder);									// Add the start time to JSONObject						
		}																				// ENDOF FOR loop computing time-stamps for each fragment in this segment
		CSFGenerator.write("Header", dateTimeBuilder.build());							// Build the DateTime JsonObject and write to the file
			
		/**
		 * Create JSONObject for the study metadata
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
		 * Create JSONObject for the clinical event annotations
		 * Assumption: The clinical annotations are stored by sequential order: <sequence, "timestamp; event">
		 * 
		 */
		JsonObjectBuilder clinicalAnnotationBuilder 									// Initialize the JsonObjectBuilder to store the clinical annotations in a for loop
				= jsonFactory.createObjectBuilder();
		for(String iteratorKey: annotationList.keySet()) 								// FOREACH time stamp in the annotationList HashMap 
			clinicalAnnotationBuilder														// Extract the annotations and populate the JsonObjectBuilder
					.add(iteratorKey, annotationList.get(iteratorKey).trim()); 
		CSFGenerator.write("ClinicalAnnotationList", clinicalAnnotationBuilder.build());
		
		/**
		 * Create Jsonobject for the channel-specific metadata 
		 */
		JsonObjectBuilder channelMetadataBuilder 										// Initialize the Jsonobjectbuilder to store channel-specific metadata
				= jsonFactory.createObjectBuilder();
		ArrayList <String> channelList = new ArrayList<>();
		for(LinkedHashMap <String, String> metadata: edfChannelMetadata) {					// FOREACH channel: extract and generate corresponding JSON objects for channel-specific metadata 
			String base = metadata.get("Label"), label = base;
			int i = -1;
			while(channelList.contains(label))
				label = base + " (" + (++i) + ")";
			channelMetadataBuilder
					.add(label, 
						jsonFactory.createObjectBuilder()
									.add("channelNumber", channelList.size())
									.add("channelTransducerType", metadata.get("Transducer Type").toString())
									.add("channelPhysicalDimension", metadata.get("Physical Dimension").toString())
									.add("channelPhysicalMinimum", metadata.get("Physical Minimum").toString())
									.add("channelPhysicalMaximum", metadata.get("Physical Maximum").toString())
									.add("channelDigitalMinimum", metadata.get("Digital Minimum").toString())
									.add("channelDigitalMaximum", metadata.get("Digital Maximum").toString())
									.add("channelPrefiltering", metadata.get("Prefiltering").toString())
									.add("channelSamplesPerDataRecord", metadata.get("Samples per Data Record").toString())
									.add("channelReserved", metadata.get("Reserved").toString()));
			channelList.add(label);
		}																				// End of for - build JSON objects for channel-specific metadata 
		channelMetadataBuilder.add("channelList", channelList.toString().replace("{", "[").replace("}", "]"));
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
			for(int j = 0; j < channelData.size(); j++)									// FOREACH Channel in the fragment
				fragmentDataBuilder
					.add(edfChannelMetadata.get(j).get("Label").toString(), 
						Arrays.toString(channelData.get(j)));								
			dataRecordBuilder.add("FragmentNumber_" + (firstFragment + i), 
									fragmentDataBuilder);
		}
		CSFGenerator.write("DataRecords", dataRecordBuilder.build());
		
		CSFGenerator.writeEnd();
		CSFGenerator.flush();
		CSFGenerator.close();
		
		return SegmentStartDate + "," + SegmentStartTime;
	}
	// ENDOF CloudwaveSignalFormatGenerator Method Definition

}
