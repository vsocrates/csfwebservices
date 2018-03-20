package edu.cwru.bmhinformatics.csfwebservices;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;

public class EDFSegment {
		
		private LinkedHashMap <String, String> edfSignalMetadata;
		private LinkedHashMap <String, String> edfAnnotations;
		private ArrayList <LinkedHashMap <String, String>> edfChannelMetadata;
		private ArrayList <byte []> edfDataFragment;
		private ArrayList <ArrayList <byte []>> edfSignalSpecificDataFragment;
		private ArrayList <ArrayList <double[]>> edfSignalSpecificDataFragmentDouble;
		private int firstFragmentSequence, lastFragmentSequence;
		private double edfepochDuration;
		
		/**
		 * Constructor method for the EDFFragment class
		 * @param studyMetadata the hashtable containing study-specific metadata
		 * @param channelMetadata the channel-specific metadata
		 * @param signalData the signal data in byte format (an array of byte [], where each byte[] has signal data corresponding 1 fragment (equal to 1 epoch by default)
		 * @return the EDFFragment object
		 * 
		 */
		EDFSegment(LinkedHashMap <String, String> signalMetadata, ArrayList <LinkedHashMap <String, String>> channelMetadata, 
				LinkedHashMap <String, String> annotations, ArrayList <byte[]> signalData, int firstFragment, int lastFragment, double epochDuration) {
			this.edfSignalMetadata = signalMetadata;                               			// Set the EDF signal metadata from input
			this.edfChannelMetadata = channelMetadata;                              		// Set the channel metadata from input
			this.edfAnnotations = annotations;
			
			this.firstFragmentSequence = firstFragment; 									// Set the sequence number of the first fragment in the EDFSegment object from input
			this.lastFragmentSequence = lastFragment;  										// Set the sequence number of the last fragment in the EDFSegment object from input
			this.edfepochDuration = epochDuration;      									// Set the duration of each epoch from input
			
			this.edfDataFragment = new ArrayList<byte[]> ();                                // Initialize the list of signal data
			for(byte[] B: signalData) 
				this.edfDataFragment.add(Arrays.copyOf(B, B.length));						// Add a copy of each array of bytes to the ArrayList of signal data - use Arrays.copyOf for single dimension array		
		} 
		// ENDOF EDFSegment Constructor
				
		// Getter methods
		public LinkedHashMap <String, String> getStudyMetadata() {return edfSignalMetadata;} 		// Getter method for study metadata variable
		public LinkedHashMap <String, String> getAnnotationList() {return edfAnnotations;}		// Getter method for clinical event annotations
		public ArrayList <LinkedHashMap <String, String>> getChannelMetadata() {return edfChannelMetadata;} // Getter method for channel-specific variable
		public ArrayList <byte[]> getSignalData() {return edfDataFragment;} 								// Getter method for binary raw signal data with EDF data records
		public ArrayList <ArrayList <byte[]>> getChannelSpecificData() 
			{return edfSignalSpecificDataFragment;}											// Getter method for binary raw channel-specific signal data 
		public ArrayList <ArrayList <double[]>> getChannelSpecificDataDouble() 
			{return edfSignalSpecificDataFragmentDouble;}
		
		public int getFirstFragmentSequence() {return firstFragmentSequence;} 				// Getter method for sequence number of first fragment of signal data
		public int getLastFragmentSequence() {return lastFragmentSequence;} 				// Getter method for sequence number of last fragment of signal data
		public double getEpochDuration() {return edfepochDuration;} 						// Getter method for epochDuration variable
		
		// Setter methods 
		public void setChannelSpecificDataDouble(ArrayList <ArrayList <double[]>> edfSignalSpecificDataFragment) {
			this.edfSignalSpecificDataFragmentDouble = edfSignalSpecificDataFragment;
		}
				
		/**
		 * Setter method for replacing EDF Data Records with signal data structured as channel oriented fragments 
		 * @param inputArray a two-dimensional array with multiple fragments of signal data, where each fragment has a byte array of data corresponding to each signal channel
		 * @return void
		 * 
		 */
		public void putChannelOrientedData(ArrayList <ArrayList <byte[]>> inputArray) {
			this.edfDataFragment = null; 	// Remove the input two-dimensional array storing the EDF Data Fragment-based signal data to reduce memory footprint
			System.gc();					// Flag JVM garbage collector to remove memory for variable		
			
			/** 
			 * In a loop,
			 * 1. For each fragment
			 * 2. For each channel, copy signal data as byte to member variable byte array
			 * 
			 */ 
			edfSignalSpecificDataFragment = new ArrayList <ArrayList <byte[]>>();			// Initialize the two-dimensional array 
			for(int fragment = 0; fragment < inputArray.size(); fragment++) {
				ArrayList <byte[]> fragmentArray = new ArrayList <byte[]> (); 					// Initialize the ArrayList to store signal data corresponding to each fragment
				for(int channel = 0; channel < inputArray.get(fragment).size(); channel++) { 	// For each fragment, copy over all signal data to channel-specific byte array
					byte[] dataFragment = new byte[inputArray.get(fragment).get(channel).length]; 	// Initialize the byte array based on size of channel-specific length
					dataFragment = Arrays.copyOf(inputArray.get(fragment).get(channel), 
							inputArray.get(fragment).get(channel).length); 							// Copy over the byte values - use Arrays.copyOf for single dimension array
					fragmentArray.add(dataFragment); 												// Add the byte array to EDFFragment object member variable
				}																					// ENDOF inner (channel?) for loop
				edfSignalSpecificDataFragment.add(fragmentArray);								// Add the fragment of signal data to array with fragments of signal data
			}																				// ENDOF outer (fragment?) for loop
			
			inputArray = null; 				// De-allocate the memory for inputArray
			System.gc(); 					// Flag JVM garbage collector to remove memory for variable
		}
		// ENDOF putChannelOrientedData Method Definition					
		
	}
	// ENDOF EDFFragment Class Definition
