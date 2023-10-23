package com.yahoo.ycsb.db;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.db.ConfigurationUtil.Configurations;
import com.yahoo.ycsb.db.NovaClient.ReturnValue;

public class NovaDBClient extends DB {
	public static enum Partition {
		RANGE, HASH
	}

	public static enum RequestType {
		GET, SCAN, PUT
	}

	private Partition partition;
	private Configurations config = null;
	private Set<Integer> serverIds = Sets.newHashSet();
	private long valueSize = 0;

	private NovaClient novaClient;

	static AtomicBoolean ONCE = new AtomicBoolean(false);
	static AtomicBoolean initialized = new AtomicBoolean(false);

	private int numberOfRecords = 0;
	boolean debug;

	private int cardinality = 0;

	long startTime = 0;
	long endTime = 0;

	int offset = 0;

	public static String generateRandomString(Random random, long valueSize) {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < valueSize; i++) {
			builder.append((char) (random.nextInt(26) + 'a'));
		}
		return builder.toString();
	}

	private int homeFragment(String key, List<LTCFragment> config) {
		int intV = Integer.parseInt(key);
		switch (partition) {
		case HASH:
			break;
		case RANGE:
			int l = 0;
			int r = config.size() - 1;
			LTCFragment home = null;
			int m = l + (r - l) / 2;
			while (l <= r) {
				m = l + (r - l) / 2;
				home = config.get(m);
				// Check if x is present at mid
				if (intV >= home.startKey && intV < home.endKey) {
					break;
				}
				// If x greater, ignore left half
				if (intV >= home.endKey)
					l = m + 1;
				// If x is smaller, ignore right half
				else
					r = m - 1;
			}
			return m;
		default:
			break;
		}
		return -1;
	}

	@Override
	public void init() throws DBException {
		super.init();
		Properties props = getProperties();
		String serversString = props.getProperty("nova_servers");
		debug = Boolean.parseBoolean(props.getProperty("debug"));
		String strPartition = props.getProperty("partition");
		valueSize = Integer.parseInt(props.getProperty("valuesize"));
		String config_path = props.getProperty("config_path");
		config = ConfigurationUtil.readConfig(config_path);
		numberOfRecords = Integer.parseInt(props.getProperty("recordcount"));
		cardinality = Integer.parseInt(props.getProperty("cardinality"));

		if (props.containsKey("offset")) {
			offset = Integer.parseInt(props.getProperty("offset"));
		}

		System.out.println("Number of fragments " + config.current().fragments.size());
		String[] ems = serversString.split(",");
		List<String> servers = new ArrayList<>();

		for (int i = 0; i < ems.length; i++) {
			servers.add(ems[i]);
			serverIds.add(i);
		}

		if ("range".equals(strPartition)) {
			partition = Partition.RANGE;
		} else if ("hash".equals(strPartition)) {
			partition = Partition.HASH;
		} else {
			System.out.println("Unknown partition algorithm.");
			System.exit(-1);
		}

		novaClient = new NovaClient(servers, debug);
		if (ONCE.compareAndSet(false, true)) {
			Enumeration keys = props.keys();
			while (keys.hasMoreElements()) {
				String key = (String) keys.nextElement();
				String value = (String) props.get(key);
				System.out.println(key + ": " + value);
			}
			System.out.println(this.toString());
			initialized.set(true);
		}
		while (!initialized.get()) {
		}
	}

	@Override
	public void cleanup() throws DBException {
		endTime = System.currentTimeMillis();

		synchronized (NovaDBClient.class) {
			long duration = endTime - startTime;
			System.out.println("Took " + duration + " to complete the benchmark");
		}
		novaClient.close();
	}

	@Override
	public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
		// if (debug) { // debug模式下 读都改成读key 0
		// 	key = "0";
		// }
		if (startTime == 0) {
			startTime = System.currentTimeMillis();
		}
		int intKey = Integer.parseInt(key) + offset;
		key = String.valueOf(intKey);
		ReturnValue retVal = null;
		while (true) {
			int clientConfigId = config.configurationId.get();
			List<LTCFragment> current = config.configs.get(clientConfigId).fragments;
			int fragmentId = homeFragment(key, current);
			int homeServerId = config.current().fragments.get(fragmentId).ltcServerId;
			retVal = novaClient.get(clientConfigId, key, homeServerId);
			if (retVal.configId != clientConfigId) {
				config.configurationId.set(retVal.configId);
				continue;
			} else {
				break;
			}
		}

		if (retVal.getValue.length() != valueSize) {
			System.out.println("FATAL: " + valueSize + " " + retVal.getValue.length());
			System.exit(-1);
		}
		result.put("field0", new StringByteIterator(retVal.getValue));
		return Status.OK;
	}

	@Override
	public Status scan(String table, String startkey, int recordcount, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result) {

		while (true) {
			int clientConfigId = config.configurationId.get();
			List<LTCFragment> current = config.configs.get(clientConfigId).fragments;
			ReturnValue retVal = null;

			int fragmentId = homeFragment(startkey, current);
			int startKeyId = Integer.parseInt(startkey);
			int cardinality = Math.min(this.cardinality, numberOfRecords - startKeyId); // 取min(cardinality, 剩下的key)
			int remainingRecords = cardinality; // 要scan的key
			List<String> keys = Lists.newArrayList();
			List<String> values = Lists.newArrayList();
			String pivotKey = startkey;

			boolean retry = false;
			if (debug) {
				System.out.println(String.format("Scan %s from server %d at cfg %d", startkey,
						current.get(fragmentId).ltcServerId, clientConfigId));
			}
			while (remainingRecords > 0) {
				int serverId = current.get(fragmentId).ltcServerId;
				retVal = novaClient.scan(clientConfigId, pivotKey, remainingRecords, serverId, keys, values);

				if (retVal.configId != clientConfigId) {
					retry = true;
					config.configurationId.set(retVal.configId);
					break;
				}

				int keyId = startKeyId + keys.size();

				remainingRecords = cardinality - keys.size();
				pivotKey = String.valueOf(keyId);
				if (remainingRecords == 0) {
					break;
				}
				while (serverId == current.get(fragmentId).ltcServerId) {
					fragmentId++;
					if (fragmentId >= current.size()) {
						System.out.println("FATAL-not-enough-fragments: " + startKeyId + " " + remainingRecords);
						System.exit(-1);
					}
				}
				if (keyId + remainingRecords >= numberOfRecords) {
					System.out.println("FATAL-too-many-records: " + startKeyId + " " + remainingRecords);
					System.exit(-1);
				}
				if (fragmentId >= current.size()) {
					System.out.println("FATAL-not-enough-fragments: " + startKeyId + " " + remainingRecords);
					System.exit(-1);
				}
			}

			if (retry) {
				continue;
			}

			if (remainingRecords < 0) {
				System.out.println("FATAL-too-many-records: " + startKeyId + " " + remainingRecords);
				System.exit(-1);
			}
			if (cardinality != keys.size() || cardinality != values.size()) {
				System.out.println(
						String.format("FATAL-wrong-records: %d %d %d", cardinality, keys.size(), values.size()));
				System.exit(-1);
			}
			for (int i = 0; i < keys.size(); i++) {
				int actualKey = Integer.parseInt(keys.get(i));
				if (actualKey != startKeyId + i) {
					System.out.println("FATAL-key-notsorted: " + actualKey);
					System.exit(-1);
				}
			}
			for (String value : values) {
				if (value.length() != valueSize) {
					System.out.println("FATAL-value: " + valueSize + " " + value.length());
					System.exit(-1);
				}
			}
			break;
		}
		return Status.OK;
	}

	@Override
	public Status update(String table, String key, HashMap<String, ByteIterator> values) {
		// if (debug) { // debug模式下 都改为对key0的写
		// 	key = "0";
		// }

		if (startTime == 0) {
			startTime = System.currentTimeMillis();
		}

		int intKey = Integer.parseInt(key) + offset;
		key = String.valueOf(intKey);
		String value = buildValue(values);

		ReturnValue retVal = null;
		while (true) {
			int clientConfigId = config.configurationId.get();
			List<LTCFragment> current = config.configs.get(clientConfigId).fragments;
			int fragmentId = homeFragment(key, current);
			int serverId = current.get(fragmentId).ltcServerId;
			retVal = novaClient.put(clientConfigId, key, value, serverId);

			if (retVal.configId != clientConfigId) {
				config.configurationId.set(retVal.configId);
				continue;
			} else {
				break;
			}
		}

		return Status.OK;
	}

	public String buildValue(HashMap<String, ByteIterator> values) { // 做成value 多退少补
		final StringBuilder builder = new StringBuilder();
		values.forEach((k, v) -> {
			builder.append(k);
			builder.append(",");
			builder.append(v.toString());
			builder.append(",");
		});
		builder.deleteCharAt(builder.length() - 1);

		String value;
		if (builder.length() > valueSize) {
			value = builder.toString().substring(0, (int) valueSize);
		} else {
			int r = (int) valueSize - builder.length();
			for (int i = 0; i < r; i++) {
				builder.append("a");
			}
			value = builder.toString();
		}
		return value;
	}

	@Override
	public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
		assert false;
		return Status.OK;
	}

	@Override
	public Status delete(String table, String key) {
		assert false;
		return null;
	}

}
