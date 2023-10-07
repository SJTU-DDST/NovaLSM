package com.yahoo.ycsb.db;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Maps;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.validation.Constants;
import com.yahoo.ycsb.validation.Entity;
import com.yahoo.ycsb.validation.Property;
import com.yahoo.ycsb.validation.Utilities;

// Validate client using Polygraph.
// Y. Alabdulkarim, M. Almaymoni and S. Ghandeharizadeh, 
// "Polygraph: A Plug-n-Play Framework to Quantify Application Anomalies," 
// in IEEE Transactions on Knowledge and Data Engineering, doi: 10.1109/TKDE.2019.2939520.
public class NovaDBValidateClient extends NovaDBClient {

	private HashMap<String, Object> transactionResults = Maps.newHashMap();
	private BufferedWriter updateLogAll = null;
	private BufferedWriter readLogAll = null;
	private int threadid;
	private long seqid = 0;
	private String logDir;
	private boolean enableLogging = false;

	public static AtomicInteger GLOBAL_ID = new AtomicInteger(-1);

	@Override
	public void init() throws DBException {
		super.init();
		if (!getProperties().containsKey("logDir")) {
			return;
		}
		this.logDir = getProperties().getProperty("logDir");
		if ("dummy".equals(logDir)) {
			return;
		}
		try {
			threadid = GLOBAL_ID.incrementAndGet();
			this.enableLogging = true;
			try {
				new File(this.logDir + "/update0-" + threadid + ".txt")
						.createNewFile();
				new File(this.logDir + "/read0-" + threadid + ".txt")
						.createNewFile();
			} catch (Exception e) {
				e.printStackTrace();
			}
			updateLogAll = new BufferedWriter(new FileWriter(
					new File(this.logDir + "/update0-" + threadid + ".txt")));
			readLogAll = new BufferedWriter(new FileWriter(
					new File(this.logDir + "/read0-" + threadid + ".txt")));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	private void generateLog(long st, long et, String operation, boolean hit) {
		String UpdateLogString = null;
		String readLogString = null;

		switch (operation) {
		case "READ":
			String key = (String) transactionResults.get("key");
			transactionResults.remove("key");
			transactionResults.remove("YCSB_KEY");

			Property[] props = new Property[transactionResults.size()];

			int i = 0;
			for (String k : transactionResults.keySet()) {
				props[i] = new Property(k.toUpperCase(),
						(String) transactionResults.get(k),
						Constants.VALUE_READ);
				i++;
			}
			Entity e = new Entity(key, Constants.ENTITY_NAMES[0], props);

			ArrayList<Entity> entities = new ArrayList<Entity>();
			entities.add(e);
			String entityLog = Utilities.generateEntitiesLog(entities);
			readLogString = Utilities.getLogString(Constants.READ_RECORD,
					Constants.READ_ACTION, threadid, seqid, st, et, entityLog);
			break;
		case "UPDATE":
			key = (String) transactionResults.get("key");
			transactionResults.remove("key");
			transactionResults.remove("YCSB_KEY");

			props = new Property[transactionResults.size()];

			i = 0;
			for (String k : transactionResults.keySet()) {
				props[i] = new Property(k.toUpperCase(),
						(String) transactionResults.get(k),
						Constants.NEW_VALUE_UPDATE);
				i++;
			}
			e = new Entity(key, Constants.ENTITY_NAMES[0], props);

			entities = new ArrayList<Entity>();
			entities.add(e);
			entityLog = Utilities.generateEntitiesLog(entities);
			UpdateLogString = Utilities.getLogString(Constants.UPDATE_RECORD,
					Constants.UPDATE_ACTION, threadid, seqid, st, et,
					entityLog);
			break;
		default:

		}
		try {
			if (readLogString != null) {
				readLogAll.write(readLogString);
			}
			if (UpdateLogString != null) {
				updateLogAll.write(UpdateLogString);
			}
		} catch (IOException e1) {
			e1.printStackTrace(System.out);
		}
		transactionResults.clear();
		seqid++;
	}

	@Override
	public Status read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		long start = System.nanoTime();
		super.read(table, key, fields, result);

		if (!this.enableLogging) {
			return Status.OK;
		}

		transactionResults.put("key", key);
		result.remove("_id");
		for (String k : result.keySet()) {
			ByteIterator o = result.get(k);
			o.reset();
			String v = o.toString();
			transactionResults.put(k, v);
		}
		generateLog(start, System.nanoTime(), Constants.READ_ACTION, false);
		return Status.OK;
	}

	@Override
	public Status update(String table, String key,
			HashMap<String, ByteIterator> values) {
		long start = System.nanoTime();
		super.update(table, key, values);

		if (!this.enableLogging) {
			return Status.OK;
		}

		String value = buildValue(values);

		transactionResults.put("key", key);
		values.remove("_id");
		transactionResults.put("field0", value);
		generateLog(start, System.nanoTime(), Constants.UPDATE_ACTION, false);
		return Status.OK;
	}

	@Override
	public void cleanup() throws DBException {
		super.cleanup();

		if (!this.enableLogging) {
			return;
		}

		try {
			readLogAll.flush();
			readLogAll.close();
			updateLogAll.flush();
			updateLogAll.close();
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}
	}
}
