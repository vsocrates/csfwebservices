package edu.cwru.bmhinformatics.csfwebservices;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParserFactory;
import javax.json.stream.JsonParsingException;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

@Path("pijn")
public class PIJNCorrelationResource {
	
	private static final int[] DAYS_IN_MONTH = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
	
	private static final Comparator <double[]> LEX = new Comparator <double[]>() {
		@Override
		public int compare(double[] o1, double[] o2) {
			int val = 0;
			for(int i = 0; i < o1.length && (val = Double.compare(o1[i], o2[i])) == 0; i++);
			return val;
		}
	};

	
	/** 
	 * RESTful methods 
	 */
	
	/**
	 * 
	 * @param data
	 * @return
	 * @throws IOException 
	 */
	@POST
	@Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
	public String getCorrelation(String inputData) throws IOException{

		JsonReader jsonParamsReader = Json.createReader(new StringReader(inputData));
		JsonObject inputParams = jsonParamsReader.readObject();
		
		String csfFileListDir = inputParams.getString("csfFileDir");
		String outputFileFolderPath = inputParams.getString("outputFilePath");
		String seizureStartTime = inputParams.getString("seizureStart");
		String seizureEndTime = inputParams.getString("seizureEnd");
		
		String lagWindowText = inputParams.getString("lagWindow");
		double lagWindow = Double.parseDouble(lagWindowText);
		//this one is comma-separated 
		String channels = inputParams.getString("channels");
		
		
		long start = System.currentTimeMillis();
		File CSFDir = new File(csfFileListDir);
		//File CSFDir[] = new File(args[0]).listFiles();
		//ArrayList <File> filenames = new ArrayList<>(Arrays.asList(CSFDir));
		ArrayList <File> filenames = new ArrayList<>();
		for(File F: CSFDir.listFiles()) { 
			int len = F.getName().length();
			if(F.getName().substring(len - 4, len).equals(".csf")) 
				filenames.add(F);
		}
		Collections.sort(filenames, CSF_COMPARE);
		
		File output = new File(outputFileFolderPath);

		String[] labels = channels.split(",");
		LinkedHashSet <String> channelSet = new LinkedHashSet <String>(Arrays.asList(labels));
		double numEntries = Math.pow(labels.length, 2.);
		double[][] matrix = new double[labels.length][labels.length];
		double[][] timeMatrix = new double[labels.length][labels.length];
		double mean = 0, stdev1 = 0; //, stdev2 = 0;
		
		BufferedWriter writer 
			= new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output)));
		//writer.write(Arrays.toString(args));
		writer.write("Input directory: " + CSFDir.getPath() + "\n");
		writer.write("Output file: " + output.getPath() + "\n");
		writer.write("Start time: " + seizureStartTime + "\n");
		writer.write("End time: " + seizureEndTime + "\n");
		writer.write("Measures: PIJN \n");
		writer.write("Channels: " + channelSet + "\n\n");		
		
		LinkedHashMap <String, ArrayList <Double>> channelData
				= CSFLagParser(filenames, seizureStartTime, seizureEndTime, channelSet, lagWindow, writer);
		for(String C: channelData.keySet()) 
			System.out.println(C + ": " + channelData.get(C).subList(0, Math.min(100, channelData.get(C).size())));

//		writer.close();
		
		for(int j = 0; j < labels.length; j++)
			for(int k = 0; k < labels.length; k++) {
				writer.write("Processing Channels " + labels[j] + " to " + labels[k] + "\n");
				System.out.println("Processing Channels " + labels[j] + " to " + labels[k]);
				//double[][] data = SDCorrelator.CSFReader(range, args[0], args[1], args[2], 
						//labels[j], labels[k], 100); // 150 is too long
				double[][] data = new double[channelData.get(labels[j]).size()][2];
				for(int i = 0; i < data.length; i++) {
					double[] pair = {channelData.get(labels[j]).get(i), channelData.get(labels[k]).get(i)};
					data[i] = pair;
				}
				double[] results = smartTimeLagCoefficient(data, 100); 
				writer.write("No Lag Coefficient: " + results[0] 
						+ "; Coefficient: " + results[1] + "; Time Lag: " + results[2] + "\n");
				System.out.print("No Lag Coefficient: " + results[0] 
						+ "; Coefficient: " + results[1] + "; Time Lag: " + results[2] + "\n");
				writer.write("Time: " + (System.currentTimeMillis() - start) + "\n");
				System.out.print("Time: " + (System.currentTimeMillis() - start) + "\n");
				matrix[j][k] = results[1];
				timeMatrix[j][k] = results[2];
				mean += matrix[j][k];
				stdev1 += (matrix[j][k] * matrix[j][k]);
			}
		printMatrix(writer, labels, matrix);
		printMatrix(writer, labels, timeMatrix);
	
		mean /= numEntries;
		stdev1 = Math.sqrt((stdev1 / numEntries) - Math.pow(mean, 2.));
		double[][] normMatrix1 = new double[labels.length][labels.length];
		for(int j = 0; j < labels.length; j++)
			for(int k = 0; k < labels.length; k++) 
				normMatrix1[j][k] = (matrix[j][k] - mean) / stdev1;
		printMatrix(writer, labels, normMatrix1);
	
		writer.write("Total Time: " + (System.currentTimeMillis() - start) + "\n");
		
		writer.close();
		
		return "Made the PIJN Corr";
		
	}
	
	
	/**
	 * Auxiliary Methods
	 */
		
	private static int fileNumber(String filename) {
		return Integer.parseInt(filename.substring(filename.indexOf("CSF-") + 4, filename.indexOf(".csf")), 10);
	}

	private static final Comparator <File> CSF_COMPARE = new Comparator <File> () {
		@Override
		public int compare(File A, File B) {
			return Integer.compare(fileNumber(A.getName()), fileNumber(B.getName()));
		}
	};
	
	public static LinkedHashMap <String, ArrayList <Double>> CSFLagParser(ArrayList <File> filenames, String startTime, String endTime, 
																			HashSet <String> channels, double lag, Writer writer) { //throws IOException {
		
		
		LinkedHashMap <String, Double> recordMap = new LinkedHashMap<>();
		LinkedHashMap <String, ArrayList <Double>> dataMap = new LinkedHashMap<>();
		for(String C: channels) dataMap.put(C, new ArrayList <Double>());
		JsonParserFactory parserFactory = Json.createParserFactory(null);
		String time = "";
		
		//int eventLength = SDAuxiliary.elapsedSeconds(startTime, endTime);
		boolean almostFinished = false;
		boolean finished = false, gap = false;
		
		//long taskTime = 0, currTime = System.nanoTime();
		System.out.println("In CSFLagParser: " + Integer.toString(filenames.size()));
		for(File F: filenames) {
			System.out.println("FileName: " + F.getPath() + "\n");
			if(finished) break;
			try{
				double startOffset = -1, endOffset = -1;
				double epoch = -1, recordLen = -1;
				boolean hasFragments = true;
				LinkedHashMap <String, String> fragments = new LinkedHashMap<>();
				JsonParser csfParser = parserFactory.createParser(new FileReader(F));
				
				//taskTime = System.nanoTime() - currTime;
				//currTime = System.nanoTime();
				
				JsonParser.Event E = csfParser.next();
				while(csfParser.hasNext() && hasFragments) { 								// WHILE the parsing stream has a next state DO
					if((E = csfParser.next()).equals(JsonParser.Event.KEY_NAME)) {				// IF the next state is a key string
						switch(csfParser.getString()) {												// SWITCH on the value of the string
						
						case "Header": 																	// CASEOF Header
							while(!(E = csfParser.next()).equals(JsonParser.Event.END_OBJECT)) 				// WHILE the object has not ended DO
								if(E.equals(JsonParser.Event.KEY_NAME)) {										// IF the next state is a key string
									String key = csfParser.getString();												// Get the key string
									/*
									if(key.equals("EpochDuration")) {												// IF the key is EpochDuration
										E = csfParser.next();															// Get the next state (value)
										epoch = Double.parseDouble(csfParser.getString());
										//taskTime = System.nanoTime() - currTime;
										//currTime = System.nanoTime();
									}
									else 
									*/
									if(key.contains("FragmentNumber_")) {											// ELSEIF the key indicates a fragment object
										String fragStartDate = "", fragStartTime = "";									// Initialize Strings to store time data
										while(!(E = csfParser.next()).equals(JsonParser.Event.END_OBJECT)) 			// WHILE the fragment object has not ended DO
											if(E.equals(JsonParser.Event.KEY_NAME))
												switch(csfParser.getString()) {
												case "startDate":
													E = csfParser.next();
													fragStartDate = csfParser.getString();
													break;
												case "startTime":
													E = csfParser.next();
													fragStartTime = csfParser.getString();
													break;
												case "epoch":
													E = csfParser.next();
													epoch = Double.parseDouble(csfParser.getString());
													break;
												}																		// ENDDOWHILE
										String fragStart = fragStartDate + "," + fragStartTime;							// Concatenate the start date and time
										double secStart = elapsedSeconds(fragStart, startTime) - lag, 
												secEnd = elapsedSeconds(fragStart, endTime) + lag;
										if(secStart < epoch && secEnd > 0) {
											fragments.put(key, fragStart);
											System.out.println(key + " is good!");
											startOffset = Math.min(0, secStart);
											endOffset = Math.max(epoch, secEnd);
										}
										else System.out.println(key + " is bad");
										if(time.length() != 0 && !gap && SDAuxiliary.elapsedSeconds(time, fragStart) != 0)
											gap = true;
										time = addSeconds(fragStart, epoch);
										//taskTime = System.nanoTime() - currTime;
										//currTime = System.nanoTime();
									}																				// ENDIF
								}																				// ENDIF 
																											// ENDDOWHILE
							hasFragments = !fragments.isEmpty();
							if(!almostFinished && hasFragments) almostFinished = true;
							if(almostFinished && !hasFragments) finished = true;
							System.out.println(F.getName() + " Header Processed");
							break;																		// ENDCASE Header
							
						case "StudyMetadata": 															// CASEOF StudyMetadata
							while(!(E = csfParser.next()).equals(JsonParser.Event.END_OBJECT))				// WHILE the object has not ended DO
								if(E.equals(JsonParser.Event.KEY_NAME) 											// IF the event is the key string "dataRecordDuration"
										&& csfParser.getString().equals("dataRecordDuration")) {
									E = csfParser.next();															// Get the next event (value)
									recordLen = Double.parseDouble(csfParser.getString());							// Store the number of seconds per data record
									//taskTime = System.nanoTime() - currTime;
									//currTime = System.nanoTime();
								}																				// ENDIF
																											// ENDDOWHILE
							System.out.println(F.getName() + " Metadata Processed");
							break;																		// ENDCASE StudyMetadata
							
						case "ChannelMetadata":															// CASEOF ChannelMetadata
							while(!(E = csfParser.next()).equals(JsonParser.Event.END_OBJECT))
								if(E.equals(JsonParser.Event.KEY_NAME) && !csfParser.getString().equals("channelList")) {
									String C = csfParser.getString();
									if(channels.contains(C)) {
										//taskTime = System.nanoTime() - currTime;
										//currTime = System.nanoTime();
									}
									while(!(E = csfParser.next()).equals(JsonParser.Event.END_OBJECT)) {
										if(channels.contains(C) 
												&& E.equals(JsonParser.Event.KEY_NAME) 
												&& csfParser.getString().equals("channelSamplesPerDataRecord")) {
											E = csfParser.next();
											recordMap.put(C, 1. / recordLen * Integer.parseInt(csfParser.getString()));
											//taskTime = System.nanoTime() - currTime;
											//currTime = System.nanoTime();
										}
									}
								}
							System.out.println(F.getName() + " Channels Processed");
							break;
							
						case "DataRecords":
							while(!(E = csfParser.next()).equals(JsonParser.Event.END_OBJECT) && !fragments.isEmpty()) // && hasData)
								if(E.equals(JsonParser.Event.KEY_NAME)) {
									if(fragments.containsKey(csfParser.getString())) {
										//String fragStart = fragments.remove(csfParser.getString());
										//startOffset = Math.max(0, elapsedSeconds(fragStart, startTime) - lag);
										//endOffset = Math.min(epoch, elapsedSeconds(fragStart, endTime) + lag);
										E = csfParser.next();
										while(!(E = csfParser.next()).equals(JsonParser.Event.END_OBJECT))
											if(E.equals(JsonParser.Event.KEY_NAME)) {
												String C = csfParser.getString();
												E = csfParser.next();
												if(channels.contains(C)) {
													//taskTime = System.nanoTime() - currTime;
													//currTime = System.nanoTime();
													
													if(E.equals(JsonParser.Event.VALUE_STRING)) {
														String[] data = csfParser.getString()
																				.replaceFirst("\\[", "")
																				.replaceFirst("\\]", "")
																				.split(",");
														int first = (int) Math.max(0, startOffset * recordMap.get(C)),
															last = (int) Math.min(data.length, endOffset * recordMap.get(C));
														//taskTime = System.nanoTime() - currTime;
														//currTime = System.nanoTime();
														Stream <String> D = Arrays.stream(data).skip(first).limit(last - first);
														//taskTime = System.nanoTime() - currTime;
														//currTime = System.nanoTime();
														
														dataMap.get(C).addAll(D.map(Double::parseDouble).collect(Collectors.toList()));
														//System.out.println(dataMap.get(C));
														//taskTime = System.nanoTime() - currTime;
														//currTime = System.nanoTime();
													}
													else if(E.equals(JsonParser.Event.START_ARRAY)) {
														E = csfParser.next();
														double a = 0;
														while(a < startOffset * recordMap.get(C) && !E.equals(JsonParser.Event.END_ARRAY)) {
															E = csfParser.next();
															a++;
														}
														while(a < endOffset * recordMap.get(C) && !E.equals(JsonParser.Event.END_ARRAY)) {
															dataMap.get(C).add(csfParser.getBigDecimal().doubleValue());
															E = csfParser.next();
															a++;
														}
													}
													else throw new JsonParsingException("Invalid Data", csfParser.getLocation());
												}
											}
									}
									else while(!(E = csfParser.next()).equals(JsonParser.Event.END_OBJECT));
								}
							System.out.println(F.getName() + " Data Processed");
							break;
						}
					}
				}
				csfParser.close();
				System.out.println(F.getName() + " " + ((hasFragments) ? "processed" : "discarded"));
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
		try {
			if(gap) {
				writer.write("WARNING!! DATA IS MISSING FOR THE DESIRED TIME INTERVAL\n\n");
			}
		}
		catch(IOException E) {
			E.printStackTrace();
		}
		return dataMap;
	}

	private static int elapsedSeconds(String S, String E) {
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
	
	public static String addSeconds(String T, double sec) {
		String[] startDate = (T.split(",")[0]).split("\\.|:"), startTime = (T.split(",")[1]).split("\\.|:");
		int startDay = Integer.parseInt(startDate[0], 10), 
			startMonth = Integer.parseInt(startDate[1], 10), 
			startYear = Integer.parseInt(startDate[2], 10),
			startHour = Integer.parseInt(startTime[0], 10), 
			startMin = Integer.parseInt(startTime[1], 10), 
			startSec = Integer.parseInt(startTime[2], 10);
		double startFrac = (startTime.length == 3) ? 0 : Double.parseDouble("0." + startTime[3]);
		startFrac += sec;
		startSec += ((int) Math.floor(startFrac));
		startFrac -= Math.floor(startFrac);
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
				+ "." + (startSec < 10 ? "0" : "") + startSec
				+ ((startFrac == 0) ? "" : Double.toString(startFrac).substring(1));
	}

	private static void printMatrix(BufferedWriter writer, String[] labels, double[][] matrix) throws IOException {
		writer.write("X" + Arrays.toString(labels).replaceFirst("\\[", ",").replaceFirst("\\]", "\n"));
		for(int x = 0; x < matrix.length; x++) 
			writer.write(labels[x] + Arrays.toString(matrix[x]).replaceFirst("\\[", ",").replaceFirst("\\]", "") + "\n");
		writer.write("\n");
	}

	public static double[] smartTimeLagCoefficient(double[][] data, int maxShift) {
		int numSamples = data.length - (2 * maxShift);
		int numBins = (int) Math.ceil(Math.log(numSamples) / Math.log(2));
		while(numSamples % numBins != 0) numBins++;
		int binSize = numSamples / numBins;
		double noLag = 0, maxVal = 0, maxTime = 0;
		for(int i = 0; i <= (2 * maxShift); i++) {
			double[][] range = new double[numSamples][2];
			for(int a = 0; a < numSamples; a++) {
				range[a][0] = data[maxShift + a][0];
				range[a][1] = data[i + a][1];
			}
			Arrays.sort(range, LEX);
			double[][] breakpoints = new double[numBins + 2][2];
			double[] slopes = new double[numBins + 1];
			breakpoints[0] = Arrays.copyOf(range[0], 2);
			breakpoints[numBins + 1] = Arrays.copyOf(range[numSamples - 1], 2);
			double sumY = 0, sumSquareY = 0;
			for(int j = 0; j < numBins; j++) {
				int start = j * binSize;
				breakpoints[j + 1][0] += range[start][0];
				for(int k = 0; k < binSize; k++) {
					double y = range[start + k][1];
					breakpoints[j + 1][1] += y; 
					sumY += y;
					sumSquareY += Math.pow(y, 2.);
				}
				breakpoints[j + 1][0] += range[start + binSize - 1][0];
				breakpoints[j + 1][0] /= 2;
				breakpoints[j + 1][1] /= binSize;
				slopes[j] = (breakpoints[j + 1][1] - breakpoints[j][1]) / (breakpoints[j + 1][0] - breakpoints[j][0]);
			}
			slopes[numBins] = (breakpoints[numBins + 1][1] - breakpoints[numBins][1]) 
									/ (breakpoints[numBins + 1][0] - breakpoints[numBins][0]);
			double averageY = sumY / numSamples;
			double totalVar = sumSquareY - (2. * sumY * averageY) + (numSamples * Math.pow(averageY, 2.)); 
			double unexplainedVar = 0;
			int leftIndex = 0;
			for(int n = 0; n < numSamples; n++) {
				if(LEX.compare(range[n], breakpoints[leftIndex + 1]) > 0) leftIndex++;
				double f = breakpoints[leftIndex][1] 
								+ (slopes[leftIndex] * (range[n][0] - breakpoints[leftIndex][0]));
				unexplainedVar += Math.pow(range[n][1] - f, 2.);
			}
			double h2 = 1. - (unexplainedVar / totalVar);
			if((maxVal = Math.max(maxVal, h2)) == h2) maxTime = i;
			if(i == maxShift) noLag = h2;
		}
		double[] result = {noLag, maxVal, maxTime - maxShift};
		return result;
	}

}
