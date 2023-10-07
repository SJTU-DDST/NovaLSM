package com.yahoo.ycsb.validation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;

public class Utilities {

	public static final String newline = System.getProperty("line.separator");

	public static String concatWithSeperator(char Seperator, String... params) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < params.length; i++) {
			sb.append(params[i]);
			if (i + 1 != params.length)
				sb.append(Seperator);
		}
		return sb.toString();
	}

	public static boolean compareValues(String value1, String value2) {
		if (isNumeric(value1) && isNumeric(value2)) {
			double a = Double.parseDouble(value1);
			double b = Double.parseDouble(value2);
			double c = Math.abs(a - b);
			return Constants.ERROR_MARGIN > c;

		} else {
			return value1.equals(value2);
		}
	}

	public static boolean isNumeric(String value) {
		try {
			Double.parseDouble(value);
			return true;
		} catch (Exception e) {
			return false;
		}

	}

	public static String concat(String... params) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < params.length; i++) {
			sb.append(params[i]);
		}
		return sb.toString();
	}

	public static String concat(String st1, char ch, String st2) {
		StringBuffer sb = new StringBuffer();
		sb.append(st1);
		sb.append(ch);
		sb.append(st2);
		return sb.toString();

	}

	public static String concat(String st1, String st2, char ch, String st3) {
		StringBuffer sb = new StringBuffer();
		sb.append(st1);
		sb.append(st2);
		sb.append(ch);
		sb.append(st3);
		return sb.toString();

	}

	public static String applyIncrements(String v1, String v2) {
		double result = Double.parseDouble(v1) + Double.parseDouble(v2);
		String str = null;
		if (result == 0)
			str = "0.00";
		else
			str = Constants.DECIMAL_FORMAT.format(result);
		return str;
	}

	// private String getStocksLogString(int ol_count) {
	// StringBuilder sb = new StringBuilder();
	// for (int i = 1; i <= ol_count; i++) {
	// String tokens[] = ((String) this.transactionResults.get("stock" + i)).split("_");
	// String stockId = generateID(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]));
	// String properties = getPropertiesString(Constants.STOCK_PROPERIES, tokens[2], Constants.NEW_VALUE_UPDATE);
	// sb.append(getEntityLogString(Constants.STOCK_ENTITY, stockId, properties));
	// if (i != ol_count)
	// sb.append(Constants.ENTITY_SEPERATOR);
	//
	// }
	// return sb.toString();
	// }
	public static String getLogString(LogRecord r) {

		StringBuilder sb = new StringBuilder();
		sb.append(r.getType());
		sb.append(Constants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(r.getActionName());
		sb.append(Constants.RECORD_ATTRIBUTE_SEPERATOR);
		String tokens[] = r.getId().split(Constants.KEY_SEPERATOR + "");
		sb.append(tokens[0]);
		sb.append(Constants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(tokens[1]);
		sb.append(Constants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(r.getStartTime());
		sb.append(Constants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(r.getEndTime());
		sb.append(Constants.RECORD_ATTRIBUTE_SEPERATOR);
		String entities = Utilities.generateEntitiesLog(r.getEntities());
		sb.append(entities);
		sb.append(newline);
		return sb.toString();
	}

	public static String getLogString(char type, String actionName, int threadId, long sequenceId, long startTime, long endTime, String... entities) {
		StringBuilder sb = new StringBuilder();
		sb.append(type);
		sb.append(Constants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(actionName);
		sb.append(Constants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(threadId+"-"+sequenceId);
		sb.append(Constants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(startTime);
		sb.append(Constants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(endTime);
		sb.append(Constants.RECORD_ATTRIBUTE_SEPERATOR);
		for (int i = 0; i < entities.length; i++) {
			sb.append(entities[i]);
			if ((i + 1) != entities.length)
				sb.append(Constants.ENTITY_SEPERATOR);
		}
		sb.append(newline);
		return sb.toString();
	}

	public static void executeRuntime(String cmd, boolean wait, String dist) {
		Process p;
		File ufile = new File(dist);
		FileWriter ufstream = null;
		try {
			ufstream = new FileWriter(ufile);
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}
		StringBuilder sb = new StringBuilder();
		BufferedWriter file = new BufferedWriter(ufstream);
		try {

			p = Runtime.getRuntime().exec(new String[] { "/bin/sh", "-c", cmd });
			if (wait) {
				InputStream stdout = p.getInputStream();
				BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));
				String line = "";
				while ((line = reader.readLine()) != null) {
					sb.append(line + "\n");
				}
				p.waitFor();
			} else
				Thread.sleep(5000);
		} catch (Exception e2) {
			e2.printStackTrace(System.out);
		}
		try {
			if (sb.length() > 0)
				sb.deleteCharAt(sb.length() - 1);
			file.write(sb.toString());
			file.flush();
			file.close();
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}
	}

	public static int getNumOfFiles(String logDir) {
		File dir = new File(logDir);
		return dir.list().length;
	}

	public static String executeRuntime(String cmd, boolean wait) {
		Process p;

		StringBuilder sb = new StringBuilder();
		try {

			p = Runtime.getRuntime().exec(new String[] { "/bin/sh", "-c", cmd });
			if (wait) {
				InputStream stdout = p.getInputStream();
				BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));
				String line = "";
				while ((line = reader.readLine()) != null) {
					sb.append(line);
				}
				p.waitFor();
			} else
				Thread.sleep(5000);
		} catch (Exception e2) {
			e2.printStackTrace(System.out);
		}

		return sb.toString();

	}

//	public static void replayBGLogs(String logDir, String topic, int numValidators, boolean multiThreads, boolean multiTopic, String ServerIP) {
//		int threadCount = getNumOfFiles(logDir) / 2;
//		if (!multiThreads) {
//			threadCount = 1;
//		}
//		KafkaProducer<String, String> kafkaProducer = null;
//		try {
//			Properties props = new Properties();
//			props.put("bootstrap.servers", ServerIP+":9092");
//			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//			//props.put(" key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//			props.put("acks", "all");
//			props.put("retries", 0);
//			props.put("batch.size", 16384);
//			props.put("linger.ms", 1);
//			props.put("buffer.memory", 33554432);
//			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//			kafkaProducer = new KafkaProducer<String, String>(props);
//		} catch (Exception ex) {
//			ex.printStackTrace(System.out);
//		}
//
//		ExecutorService exec = Executors.newFixedThreadPool(threadCount);
//		Set<Future<Long>> set = new HashSet<Future<Long>>();
//		for (int i = 0; i < threadCount; i++) {
//			ReplayThread t = new ReplayThread(i + "", kafkaProducer, numValidators, logDir, topic, multiThreads, multiTopic);
//
//			Future<Long> future = exec.submit(t);
//			set.add(future);
//		}
//
//		try {
//			long sum = 0;
//			for (Future<Long> future : set) {
//				sum += future.get();
//			}
//			System.out.println("The sum of the logs:" + sum);
//			System.out.println("Done");
//			kafkaProducer.close();
//		} catch (Exception e) {
//			e.printStackTrace(System.out);
//		}
//
//	}

//	static class ReplayThread implements Callable<Long> {
//		String id;
//		KafkaProducer<String, String> producer;
//		int numValidators;
//		String logDir;
//		String topic;
//		boolean multiThreads;
//		boolean multiTopic;
//
//		public ReplayThread(String id, KafkaProducer<String, String> producer, int validators, String dir, String topic, boolean multi, boolean multiTopic) {
//			this.id = id;
//			this.producer = producer;
//			this.numValidators = validators;
//			this.logDir = dir;
//			this.topic = topic;
//			multiThreads = multi;
//			this.multiTopic = multiTopic;
//
//		}
//
//		@Override
//		public Long call() throws Exception {
//			int threadCount = 2;
//			if (!multiThreads) {
//				threadCount = getNumOfFiles(logDir);
//
//			}
//			FileInputStream[] fstreams = new FileInputStream[threadCount];
//			DataInputStream[] dataInStreams = new DataInputStream[threadCount];
//			BufferedReader[] bReaders = new BufferedReader[threadCount];
//			long logsCount = 0;
//			HashSet<String> logs = new HashSet<String>();
//			for (int i = 0; i < threadCount; i = i + 2) {
//
//				try {
//					int machineid = 0;
//					if (!multiThreads) {
//						id = i / 2 + "";
//					}
//					fstreams[i] = new FileInputStream(logDir + "/read" + machineid + "-" + id + ".txt");
//					dataInStreams[i] = new DataInputStream(fstreams[i]);
//					bReaders[i] = new BufferedReader(new InputStreamReader(dataInStreams[i]));
//
//					fstreams[i + 1] = new FileInputStream(logDir + "/update" + machineid + "-" + id + ".txt");
//					dataInStreams[i + 1] = new DataInputStream(fstreams[i + 1]);
//					bReaders[i + 1] = new BufferedReader(new InputStreamReader(dataInStreams[i + 1]));
//				} catch (FileNotFoundException e) {
//					e.printStackTrace(System.out);
//					System.out.println("Log file not found " + e.getMessage());
//				}
//			}
//
//			// ==================================================================
//			int seq = 0;
//			try {
//				String line = null;
//				boolean allDone = false;
//				LogRecord[] records = new LogRecord[threadCount];
//				while (!allDone) {
//					allDone = true;
//					for (int i = 0; i < threadCount; i++) {
//						if (records[i] == null) {
//							if ((line = bReaders[i].readLine()) != null) {
//								records[i] = LogRecord.createLogRecord(line);
//								allDone = false;
//							}
//						} else {
//							allDone = false;
//						}
//					}
//					LogRecord currentRecord = null;
//					if (allDone) {
//						;
//						// if (ReadWrite.size() != 0) {
//						// allDone = false;
//						// currentRecord = ReadWrite.get(0);
//						// ReadWrite.remove(0);
//						// }
//					} else {
//						currentRecord = getEarilestRecord(records);
//
//					}
//					if (!allDone) {
//						// if(currentRecord.getId().equals("57-0"))
//						// System.out.println();
//						while (logs.contains(currentRecord.getId())) {
//							System.out.println("Log:" + currentRecord.getId() + " already exit");
//
//							currentRecord.setId("101-" + seq);
//							seq++;
//							// System.out.println("Log:"+ currentRecord.getId() +" already exit");
//							// System.exit(0);
//						}
//						logs.add(currentRecord.getId());
//						sendToKafka(currentRecord);
//						logsCount++;
//					}
//				}
//			} catch (Exception e) {
//				e.printStackTrace(System.out);
//				System.exit(0);
//			}
//			try {
//				for (int i = 0; i < threadCount; i++) {
//					if (dataInStreams[i] != null)
//						dataInStreams[i].close();
//					if (bReaders[i] != null)
//						bReaders[i].close();
//				}
//			} catch (IOException e) {
//				e.printStackTrace(System.out);
//			}
//
//			return logsCount;
//		}
//
//		private void sendToKafka(LogRecord currentRecord) {
//			if (currentRecord.getId().equals("41-24"))
//				System.out.println();
//			Entity[] entities = currentRecord.getEntities();
//			if (entities.length > 1) {
//				System.out.println("Error: Expecting one entity");
//				System.exit(0);
//			}
//			int key = Integer.parseInt(entities[0].key);
//
//			// R_S = (TopicId % NumVal)
//			// R_E = (TopicId % NumVal) + (NumVal)
//			// W_S = (TopicId % NumVal) + (2 * NumVal)
//			// W_E = (TopicId % NumVal) + (3 * NumVal)
//
//			String tempTopic = topic;
//			if (multiTopic) {
//				numValidators = 2;
//				tempTopic += (key % numValidators);
//				numValidators = 1;
//			}
//			int partition = (key % numValidators);
//			if (currentRecord.getType() != ValidationConstants.READ_RECORD)
//				partition = (key % numValidators) + (2 * numValidators);
//
//			producer.send(new ProducerRecord<String, String>(tempTopic, partition, currentRecord.getId(), Long.toString(currentRecord.getStartTime())));
//			partition = (key % numValidators) + numValidators;
//			if (currentRecord.getType() != ValidationConstants.READ_RECORD)
//				partition = (key % numValidators) + (3 * numValidators);
//			String value = Utilities.getLogString(currentRecord);
//			producer.send(new ProducerRecord<String, String>(tempTopic, partition, currentRecord.getId(), value));
//
//		}
//
//	}

	public static LogRecord getEarilestRecord(LogRecord[] records) {
		if (records == null || records.length == 0) {
			System.out.println("ERROR: (records == null || records.length == 0) returned true");
			System.exit(0);
		}
		LogRecord min = records[0];
		int index = 0;
		for (int i = 1; i < records.length; i++) {
			if (records[i] == null)
				continue;
			if (min == null || min.getStartTime() > records[i].getStartTime()) {
				index = i;
				min = records[i];
			}
		}
		records[index] = null;
		return min;
	}

	public static String getStaleLogString(char type, String actionName, String id, long startTime, long endTime, long readOffset, long updateOffset, String... entities) {
		StringBuilder sb = new StringBuilder();
		sb.append(type);
		sb.append(Constants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(actionName);
		sb.append(Constants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(id);
		sb.append(Constants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(startTime);
		sb.append(Constants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(endTime);
		sb.append(Constants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(readOffset);
		sb.append(Constants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(updateOffset);
		sb.append(Constants.RECORD_ATTRIBUTE_SEPERATOR);
		for (int i = 0; i < entities.length; i++) {
			sb.append(entities[i]);
			if ((i + 1) != entities.length)
				sb.append(Constants.ENTITY_SEPERATOR);
		}
		sb.append(newline);
		return sb.toString();
	}

	public static String getEntityLogString(String name, String key, String properies) {
		StringBuilder sb = new StringBuilder();
		sb.append(name);
		sb.append(Constants.ENTITY_ATTRIBUTE_SEPERATOR);
		sb.append(key);
		sb.append(Constants.ENTITY_ATTRIBUTE_SEPERATOR);
		sb.append(properies);
		return sb.toString();
	}

	public static final String ValidationLogDir = "C:/Users/MR1/Documents/Validation/logs";




	public static String generateEntitiesLog(Entity[] entities) {
		// CUS;1-2-493;BALANCE:-374.44:N#YTD_P:374.44:N#P_CNT:2:N
		StringBuilder sb = new StringBuilder();
		try {

			for (Entity e : entities) {
				sb.append(concat(e.name, Constants.ENTITY_ATTRIBUTE_SEPERATOR + "", e.key, Constants.ENTITY_ATTRIBUTE_SEPERATOR + ""));
				for (Property p : e.getProperties()) {
					sb.append(concat(p.getName(), Constants.PROPERY_ATTRIBUTE_SEPERATOR + "", p.getValue(), Constants.PROPERY_ATTRIBUTE_SEPERATOR + "", p.getType() + "" + Constants.PROPERY_SEPERATOR + ""));

				}
				sb.deleteCharAt(sb.length() - 1); // remove last #
				sb.append(concat(Constants.ENTITY_SEPERATOR + ""));
			}

			sb.deleteCharAt(sb.length() - 1); // remove last &
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		}

		return sb.toString();
	}

	public static String generateEntitiesLog(ArrayList<Entity> entities) {
		// CUS;1-2-493;BALANCE:-374.44:N#YTD_P:374.44:N#P_CNT:2:N
		StringBuilder sb = new StringBuilder();
		try {

			for (Entity e : entities) {
				sb.append(concat(e.name, Constants.ENTITY_ATTRIBUTE_SEPERATOR + "", e.key, Constants.ENTITY_ATTRIBUTE_SEPERATOR + ""));
				for (Property p : e.getProperties()) {
					sb.append(concat(p.getName(), Constants.PROPERY_ATTRIBUTE_SEPERATOR + "", p.getValue(), Constants.PROPERY_ATTRIBUTE_SEPERATOR + "", p.getType() + "" + Constants.PROPERY_SEPERATOR + ""));

				}
				sb.deleteCharAt(sb.length() - 1); // remove last #
				sb.append(concat(Constants.ENTITY_SEPERATOR + ""));
			}

			sb.deleteCharAt(sb.length() - 1); // remove last &
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		}

		return sb.toString();
	}








	public static String getPropertiesString(String[] properiesNames, Object... params) {
		StringBuilder sb = new StringBuilder();
		int j = 0;
		for (int i = 0; i < properiesNames.length; i++, j++) {
			sb.append(properiesNames[i]);
			sb.append(Constants.PROPERY_ATTRIBUTE_SEPERATOR);
			sb.append(params[j]);
			sb.append(Constants.PROPERY_ATTRIBUTE_SEPERATOR);
			sb.append(params[++j]);
			if ((i + 1) != properiesNames.length)
				sb.append(Constants.PROPERY_SEPERATOR);
		}
		return sb.toString();
	}


	public static String getPropertiesString2(String[] pNames, String[] pValues, char[] pTypes) {
		StringBuilder sb = new StringBuilder();
		boolean firstOne = true;
		for (int i = 0; i < pNames.length; i++) {
			if (pTypes[i] != Constants.NO_READ_UPDATE) {
				if (!firstOne) {
					sb.append(Constants.PROPERY_SEPERATOR);
				} else {
					firstOne = false;
				}
				sb.append(pNames[i]);
				sb.append(Constants.PROPERY_ATTRIBUTE_SEPERATOR);
				sb.append(pValues[i]);
				sb.append(Constants.PROPERY_ATTRIBUTE_SEPERATOR);
				sb.append(pTypes[i]);
			}
		}
		return sb.toString();
	}

	public static String getLogString(char type, String actionName, int threadId, int sequenceId, long startTime, long endTime, ArrayList<String> customers, ArrayList<String> orders) {
		StringBuilder sb = new StringBuilder();
		sb.append(type);
		sb.append(Constants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(actionName);
		sb.append(Constants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(threadId+"-"+sequenceId);
		sb.append(Constants.RECORD_ATTRIBUTE_SEPERATOR);
	
		sb.append(startTime);
		sb.append(Constants.RECORD_ATTRIBUTE_SEPERATOR);
		sb.append(endTime);
		sb.append(Constants.RECORD_ATTRIBUTE_SEPERATOR);
		for (int i = 0; i < customers.size(); i++) {
			sb.append(customers.get(i));
			sb.append(Constants.ENTITY_SEPERATOR);
			sb.append(orders.get(i));
			if ((i + 1) != customers.size())
				sb.append(Constants.ENTITY_SEPERATOR);
		}
		sb.append(newline);
		return sb.toString();
	}

}
