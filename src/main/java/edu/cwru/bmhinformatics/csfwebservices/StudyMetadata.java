package edu.cwru.bmhinformatics.csfwebservices;

public class StudyMetadata {
	
	private String fileName;
	private String dataFormatVersion;
	
	/** 
	 * EDF+ spec for patient ID
	 * The 'local patient identification' field must start with the subfields (subfields do not contain, but are separated by, spaces):
	 * - the code by which the patient is known in the hospital administration.
	 * - sex (English, so F or M).
	 * - birthdate in dd-MMM-yyyy format using the English 3-character abbreviations of the month in capitals. 02-AUG-1951 is OK, while 2-AUG-1951 is not.
	 * - the patients name. 
	 * 
	 */
	private String localPatientIdentification;
	/**
	 * The 'local recording identification' field must start with the subfields (subfields do not contain, but are separated by, spaces):
	 * - The text 'Startdate'.
	 * - The startdate itself in dd-MMM-yyyy format using the English 3-character abbreviations of the month in capitals.
	 * - The hospital administration code of the investigation, i.e. EEG number or PSG number.
	 * - A code specifying the responsible investigator or technician.
	 * - A code specifying the used equipment. 
	 * 
	 */
	private String localRecordingIdentification;
	
	/**
	 * EDF+ specs: The 'startdate' and 'starttime' fields in the header should contain only characters 0-9, 
	 * and the period (.) as a separator, for example "02.08.51". In the 'startdate', use 1985 as
	 * a clipping date in order to avoid the Y2K problem. So, the years 1985-1999 must be represented
	 * by yy=85-99 and the years 2000-2084 by yy=00-84. After 2084, yy must be 'yy' and only item 4
	 * of this paragraph defines the date. 
	 */
	private String recordingStartDate;
	private String recordingStartTime;
	
	private String numHeaderBytes;
	
	/**
	 * EDF+ Spec: The 'number of data records' can only be -1 during recording. As soon as the 
	 * file is closed, the correct number is known and must be entered. 
	 */
	private String numDataRecords;
	private String dataRecordsDuration;
	private String numSignals;
	
	/**
	 * @return the fileName
	 */
	public String getFileName() {
		return fileName;
	}
	/**
	 * @return the dataFormatVersion
	 */
	public String getDataFormatVersion() {
		return dataFormatVersion;
	}
	/**
	 * @return the localPatientIdentification
	 */
	public String getLocalPatientIdentification() {
		return localPatientIdentification;
	}
	/**
	 * @return the localRecordingIdentification
	 */
	public String getLocalRecordingIdentification() {
		return localRecordingIdentification;
	}
	/**
	 * @return the recordingStartDate
	 */
	public String getRecordingStartDate() {
		return recordingStartDate;
	}
	/**
	 * @return the recordingStartTime
	 */
	public String getRecordingStartTime() {
		return recordingStartTime;
	}
	/**
	 * @return the numHeaderBytes
	 */
	public String getNumHeaderBytes() {
		return numHeaderBytes;
	}
	/**
	 * @return the numDataRecords
	 */
	public String getNumDataRecords() {
		return numDataRecords;
	}
	/**
	 * @return the dataRecordsDuration
	 */
	public String getDataRecordsDuration() {
		return dataRecordsDuration;
	}
	/**
	 * @return the numSignals
	 */
	public String getNumSignals() {
		return numSignals;
	}
	
	
	
}
