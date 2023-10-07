package com.yahoo.ycsb.db;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ConfigurationUtil {

	public static class Configuration {
		List<LTCFragment> fragments = Lists.newArrayList();
		List<Integer> ltcs = Lists.newArrayList();
		List<Integer> stocs = Lists.newArrayList();
		long startTimeInSeconds = 0;

		public List<LTCFragment> copy() {
			List<LTCFragment> copy = Lists.newArrayList();
			for (LTCFragment frag : fragments) {
				copy.add(frag.copy());
			}
			return copy;
		}
	}

	public static class Configurations {
		List<Configuration> configs = Lists.newArrayList();
		AtomicInteger configurationId = new AtomicInteger(0);
		Map<Integer, String> servers = Maps.newHashMap();

		public Configuration current() {
			return configs.get(configurationId.intValue());
		}
	}

	public static Configurations readConfig(String configPath) {
		Configurations config = new Configurations();
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(configPath)));
			String line = null;

			Configuration cfg = new Configuration();
			while ((line = br.readLine()) != null) {
				if (line.contains("config")) {
					if (!cfg.fragments.isEmpty()) {
						config.configs.add(cfg);
					}
					cfg = new Configuration();

					line = br.readLine();
					assert line != null;
					String[] ltcs = line.split(",");
					for (String ltc : ltcs) {
						cfg.ltcs.add(Integer.parseInt(ltc));
					}
					line = br.readLine();
					assert line != null;
					String[] stocs = line.split(",");
					for (String stoc : stocs) {
						cfg.stocs.add(Integer.parseInt(stoc));
					}
					line = br.readLine();
					assert line != null;
					cfg.startTimeInSeconds = Long.parseLong(line);
					continue;
				}

				String[] ems = line.split(",");
				LTCFragment frag = new LTCFragment();
				cfg.fragments.add(frag);
				frag.startKey = Long.parseLong(ems[0]);
				frag.endKey = Long.parseLong(ems[1]);
				frag.ltcServerId = Integer.parseInt(ems[2]);
				frag.dbId = Integer.parseInt(ems[3]);

				for (int i = 4; i < ems.length; i += 1) {
					frag.stocLogReplicaServerIds.add(Integer.parseInt(ems[i]));
				}
			}
			config.configs.add(cfg);
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return config;
	}

	public static List<LTCFragment> generateConfig(String outputPath, int nRecords, int nLTCServers,
			int nRangesPerServer) throws Exception {
		int tRanges = nRangesPerServer * nLTCServers;
		int nRecordsPerRange = nRecords / tRanges;
		List<LTCFragment> fragments = Lists.newArrayList();
		int startKey = 0;
		int dbId = 0;

		for (int i = 0; i < nLTCServers; i++) {
			for (int j = 0; j < nRangesPerServer; j++) {
				LTCFragment frag = new LTCFragment();
				frag.startKey = startKey;
				frag.endKey = startKey + nRecordsPerRange;
				frag.ltcServerId = i;
				frag.dbId = dbId;
				dbId++;
				fragments.add(frag);
				startKey += nRecordsPerRange;
			}
		}

		// Last range.
		fragments.get(fragments.size() - 1).endKey = nRecords;
		return fragments;
	}

	private static void writeConfig(String outputPath, String arch, int nRecords, int nLTCServers, int nStoCServers,
			int nRangesPerServer, double zipfian, int nrecordsPerRead, Configurations configs) throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(
				String.format("%s/nova-%s-nrecords-%d-nltc-%d-nstoc-%d-nranges-%d-zipfian-%.2f-read-%d", outputPath,
						arch, nRecords, nLTCServers, nStoCServers, nRangesPerServer, zipfian, nrecordsPerRead))));
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < configs.configs.size(); i++) {
			Configuration config = configs.configs.get(i);
			builder.append("config-" + i);
			builder.append("\n");
			for (int j = 0; j < config.ltcs.size(); j++) {
				builder.append(config.ltcs.get(j));
				builder.append(",");
			}
			builder = builder.deleteCharAt(builder.length() - 1);
			builder.append("\n");

			for (int j = 0; j < config.stocs.size(); j++) {
				builder.append(config.stocs.get(j));
				builder.append(",");
			}
			builder = builder.deleteCharAt(builder.length() - 1);
			builder.append("\n");
			builder.append(config.startTimeInSeconds);
			builder.append("\n");

			for (LTCFragment frag : config.fragments) {
				builder.append(frag.toConfigString());
				builder.append("\n");
			}
		}

		bw.write(builder.toString());
		bw.flush();
		bw.close();
	}

	private static List<LTCFragment> constructRanges(TreeMap<Integer, Double> sortedMap, double totalShares, int lower,
			int upper, int numberOfFragments) {
		assert upper - lower > 1;
		assert numberOfFragments > 1;
		List<LTCFragment> tmp_subranges = Lists.newArrayList();
		double sharePerSubRange = totalShares / numberOfFragments;
		double currentRate = 0;

		LTCFragment currentRange = new LTCFragment();
		currentRange.startKey = lower;
		for (Entry<Integer, Double> entry : sortedMap.entrySet()) {
			assert entry.getKey() >= lower;
			assert entry.getKey() < upper;

			double rate = entry.getValue();
			if (currentRate + rate > sharePerSubRange) {
				if (currentRange.startKey == entry.getKey()) {
					currentRate += rate;
					currentRange.refCount = currentRate;
					currentRange.endKey = entry.getKey() + 1;
					tmp_subranges.add(currentRange);
					currentRange = new LTCFragment();

					currentRange.startKey = entry.getKey() + 1;
					if (tmp_subranges.size() + 1 == numberOfFragments) {
						break;
					}
					currentRate = 0;
					totalShares -= rate;
					sharePerSubRange = totalShares / (numberOfFragments - tmp_subranges.size());
					continue;
				} else {
					currentRange.refCount = currentRate;
					currentRange.endKey = entry.getKey();

					tmp_subranges.add(currentRange);
					currentRange = new LTCFragment();

					currentRange.startKey = entry.getKey();
					if (tmp_subranges.size() + 1 == numberOfFragments) {
						break;
					}
					currentRate = 0;
					sharePerSubRange = totalShares / (numberOfFragments - tmp_subranges.size());
				}
			}
			currentRate += rate;
			totalShares -= rate;
		}
		if (currentRange.startKey < upper) {
			currentRange.refCount = currentRate;
			tmp_subranges.add(currentRange);
		}
		assert tmp_subranges.size() <= numberOfFragments;

		tmp_subranges.get(0).startKey = lower;
		tmp_subranges.get(tmp_subranges.size() - 1).endKey = upper;
		return tmp_subranges;
	}

	static List<Double> refs = Lists.newArrayList();

	public static void readZipfDist(int nRecords, double zipf) throws Exception {
		BufferedReader br = new BufferedReader(
				new FileReader(new File(String.format("/tmp/zipfian-%d-%.2f", nRecords, zipf))));
		String line = null;
		while ((line = br.readLine()) != null) {
			double refcount = Double.parseDouble(line);
			refs.add(refcount);
		}
		br.close();
	}

	public static List<LTCFragment> updateConfigurationBasedOnLoad(int nRecords, int numberOfLTCs,
			List<LTCFragment> fragments) throws Exception {
		Map<Integer, Double> serverLoad = Maps.newHashMap();
		final List<LTCFragment> balanceLoadFragments = Lists.newArrayList();
		for (LTCFragment frag : fragments) {
			balanceLoadFragments.add(frag.copy());
		}
		double sum = 0;
		int fragId = 0;
		for (LTCFragment frag : balanceLoadFragments) {
			serverLoad.compute(frag.ltcServerId, (k, v) -> {
				if (v == null) {
					return frag.refCount;
				}
				return v + frag.refCount;
			});
			System.out.println(String.format("%d:%d", fragId, (int) frag.refCount));
			sum += frag.refCount;
			fragId++;
		}
		for (int ltcId = 0; ltcId < numberOfLTCs; ltcId++) {
			System.out.println("LTC " + ltcId + " " + serverLoad.get(ltcId) / sum);
		}

		double loadPerLTC = sum / numberOfLTCs;
		Set<Integer> movedFrags = Sets.newHashSet();
		int ltcId = 0;
		// Move fragment to other LTCs.
		for (fragId = 0; fragId < balanceLoadFragments.size(); fragId++) {
			LTCFragment frag = balanceLoadFragments.get(fragId);
			if (frag.ltcServerId != ltcId) {
				continue;
			}

			if (!movedFrags.contains(fragId)) {
				for (int otherLTCId = ltcId + 1; otherLTCId < numberOfLTCs; otherLTCId++) {
					if (serverLoad.get(otherLTCId) + frag.refCount > loadPerLTC) {
						continue;
					}
					System.out.println("Migrate fragment " + frag.toString() + " " + otherLTCId);
					serverLoad.compute(frag.ltcServerId, (k, v) -> {
						return v - frag.refCount;
					});
					serverLoad.compute(otherLTCId, (k, v) -> {
						return v + frag.refCount;
					});
					frag.ltcServerId = otherLTCId;
					movedFrags.add(fragId);
					break;
				}
				if (serverLoad.get(ltcId) < loadPerLTC) {
					break;
				}
			}
		}
		for (ltcId = 0; ltcId < numberOfLTCs; ltcId++) {
			System.out.println("LTC " + ltcId + " " + serverLoad.get(ltcId) / sum);
		}
		System.out.println("Migrated " + movedFrags.size() + " Total " + fragments.size());
		return balanceLoadFragments;
	}

	public static void main(String[] args) throws Exception {
		int index = 0;
		String outputPath = args[index++];
		String arch = args[index++];
		int nRecords = Integer.parseInt(args[index++]);
		int nLTCServers = Integer.parseInt(args[index++]);
		int nStoCs = Integer.parseInt(args[index++]);
		int nRangesPerServer = Integer.parseInt(args[index++]);

		if ("shared".equals(arch)) {
			ComputeSharedConfig(outputPath, arch, nRecords, nLTCServers, nStoCs, nRangesPerServer);
		} else if ("migration".equals(arch)) {
			double zipf = Double.parseDouble(args[index++]);
			int nrecordsPerRead = Integer.parseInt(args[index++]);
			readZipfDist(nRecords, zipf);
			ComputeLTCMigrationConfig(zipf, nrecordsPerRead, outputPath, arch, nRecords, nLTCServers, nStoCs,
					nRangesPerServer);
		} else if ("addltc".equals(arch)) {
			ComputeAddLTCConfig(outputPath, arch, nRecords, nLTCServers, nStoCs, nRangesPerServer);
		} else if ("rmltc".equals(arch)) {
			ComputeRemoveLTCConfig(outputPath, arch, nRecords, nLTCServers, nStoCs, nRangesPerServer);
		} else if ("addstoc".equals(arch)) {
			ComputeAddStoCConfig(outputPath, arch, nRecords, nLTCServers, nStoCs, nRangesPerServer);
		} else if ("rmstoc".equals(arch)) {
			ComputeRemoveStoCConfig(outputPath, arch, nRecords, nLTCServers, nStoCs, nRangesPerServer);
		} else if ("elastic".equals(arch)) {
			ComputeElasticConfig(outputPath, arch, nRecords, nLTCServers, nStoCs, nRangesPerServer);
		} else if ("simpleelastic".equals(arch)) {
			ComputeSimpleElasticConfig(outputPath, arch, nRecords, nLTCServers, nStoCs, nRangesPerServer);
		} else if ("workloada".equals(arch)) {
			ComputeRW50Config(outputPath, arch, nRecords, nLTCServers, nStoCs, nRangesPerServer);
		} else if ("workloade".equals(arch)) {
			ComputeSW50Config(outputPath, arch, nRecords, nLTCServers, nStoCs, nRangesPerServer);
		}
	}

	private static List<Double> Real() throws Exception {
		String file = "/home/haoyuhua/Documents/nova/NovaLSM-YCSB-Client/jdbc/scan_stats";
		BufferedReader br = new BufferedReader(new FileReader(new File(file)));
		String line = null;
		int key = 0;
		List<Double> items = Lists.newArrayList();
		while ((line = br.readLine()) != null) {
			String[] ems = line.split(",");
			for (String em : ems) {
				String value = em.split("-")[0].replace("[", "");
				System.out.println(String.format("%d:%s", key, value));
				key++;
				items.add(Double.parseDouble(value));
			}
		}
		br.close();
		return items;
	}

	private static void ComputeLTCMigrationConfig(double zipf, int nrecordsPerRead, String outputPath, String arch,
			int nRecords, int nLTCServers, int nStoCs, int nRangesPerServer) throws Exception, IOException {
		List<Double> new_refs = Lists.newArrayList(refs);
		List<LTCFragment> configuration = Lists.newArrayList();
		int nRecordsPerLTC = nRecords / nLTCServers;
		int dbid = 0;
		for (int ltcId = 0; ltcId < nLTCServers; ltcId++) {
			TreeMap<Integer, Double> sortedMap = Maps.newTreeMap();
			double totalShares = 0;
			int lower = ltcId * nRecordsPerLTC;
			int upper = (ltcId + 1) * nRecordsPerLTC;
			if (ltcId == nLTCServers - 1) {
				upper = nRecords;
			}
			for (int recordId = lower; recordId < upper; recordId++) {
				sortedMap.put(recordId, new_refs.get(recordId));
				totalShares += new_refs.get(recordId);
			}
			System.out.println("Create fragment for server " + ltcId + " " + totalShares + " " + lower + " " + upper);
			List<LTCFragment> fragments = constructRanges(sortedMap, totalShares, lower, upper, nRangesPerServer);
			for (LTCFragment frag : fragments) {
				frag.dbId = dbid++;
				frag.ltcServerId = ltcId;
			}
			configuration.addAll(fragments);
		}

		// for (int i = 0; i < configuration.size(); i++) {
		// configuration.get(i).refCount = 0;
		// for (int key = (int) configuration.get(i).startKey; key <
		// configuration.get(i).endKey; key++) {
		// configuration.get(i).refCount += new_refs.get(key);
		// }
		// }

		// List<Double> items = Real();
		// for (int i = 0; i < items.size(); i++) {
		// configuration.get(i).refCount = items.get(i);
		// }
		List<LTCFragment> balanced = updateConfigurationBasedOnLoad(nRecords, nLTCServers, configuration);
		Configurations configs = new Configurations();
		{
			Configuration cfg1 = new Configuration();
			cfg1.fragments = configuration;
			cfg1.startTimeInSeconds = 0;
			for (int i = 0; i < nLTCServers; i++) {
				cfg1.ltcs.add(i);
			}
			for (int i = 0; i < nStoCs; i++) {
				cfg1.stocs.add(nLTCServers + i);
			}
			configs.configs.add(cfg1);
		}
		{
			Configuration cfg2 = new Configuration();
			cfg2.fragments = balanced;
			cfg2.startTimeInSeconds = 60;
			for (int i = 0; i < nLTCServers; i++) {
				cfg2.ltcs.add(i);
			}
			for (int i = 0; i < nStoCs; i++) {
				cfg2.stocs.add(nLTCServers + i);
			}
			configs.configs.add(cfg2);
		}
		writeConfig(outputPath, arch, nRecords, nLTCServers, nStoCs, nRangesPerServer, zipf, nrecordsPerRead, configs);
	}

	private static void ComputeSharedConfig(String outputPath, String arch, int nRecords, int nLTCServers, int nStoCs,
			int nRangesPerServer) throws Exception, IOException {
		List<LTCFragment> fragments = generateConfig(outputPath, nRecords, nLTCServers, nRangesPerServer);
		Configurations configs = new Configurations();
		Configuration cfg = new Configuration();
		cfg.fragments = fragments;
		for (int i = 0; i < nLTCServers; i++) {
			cfg.ltcs.add(i);
		}
		for (int i = 0; i < nStoCs; i++) {
			cfg.stocs.add(nLTCServers + i);
		}
		configs.configs.add(cfg);
		writeConfig(outputPath, arch, nRecords, nLTCServers, nStoCs, nRangesPerServer, 0, 1, configs);
	}

	private static void ComputeAddLTCConfig(String outputPath, String arch, int nRecords, int nLTCServers, int nStoCs,
			int nRangesPerServer) throws Exception, IOException {
		List<LTCFragment> fragments = generateConfig(outputPath, nRecords, nLTCServers, nRangesPerServer);
		Configurations configs = new Configurations();
		{
			Configuration cfg = new Configuration();
			cfg.fragments = fragments;
			for (int i = 0; i < nLTCServers - 1; i++) {
				cfg.ltcs.add(i);
			}
			for (int i = 0; i < nStoCs; i++) {
				cfg.stocs.add(nLTCServers + i);
			}
			configs.configs.add(cfg);
		}

		{
			List<LTCFragment> newFragments = generateConfig(outputPath, nRecords, nLTCServers, nRangesPerServer);
			int newRangesPerLTC = newFragments.size() / (nLTCServers + 1);
			int migratePerLTC = nRangesPerServer - newRangesPerLTC;
			int[] migrate = new int[nLTCServers];
			for (int i = 0; i < newFragments.size(); i++) {
				LTCFragment frag = newFragments.get(i);
				if (migrate[frag.ltcServerId] == migratePerLTC) {
					continue;
				}
				migrate[frag.ltcServerId]++;
				System.out.println(String.format("Migrate fragment %s from %d to %d", frag.toString(), frag.ltcServerId,
						nLTCServers));
				frag.ltcServerId = nLTCServers;
			}
			Configuration cfg = new Configuration();
			cfg.fragments = newFragments;
			for (int i = 0; i < nLTCServers; i++) {
				cfg.ltcs.add(i);
			}
			for (int i = 0; i < nStoCs; i++) {
				cfg.stocs.add(nLTCServers + i);
			}
			configs.configs.add(cfg);
		}
		writeConfig(outputPath, arch, nRecords, nLTCServers, nStoCs, nRangesPerServer, 0, 1, configs);
	}

	private static void ComputeRemoveLTCConfig(String outputPath, String arch, int nRecords, int nLTCServers,
			int nStoCs, int nRangesPerServer) throws Exception, IOException {
		List<LTCFragment> fragments = generateConfig(outputPath, nRecords, nLTCServers, nRangesPerServer);
		Configurations configs = new Configurations();
		{
			Configuration cfg = new Configuration();
			cfg.fragments = fragments;
			for (int i = 0; i < nLTCServers; i++) {
				cfg.ltcs.add(i);
			}
			for (int i = 0; i < nStoCs; i++) {
				cfg.stocs.add(nLTCServers + i);
			}
			configs.configs.add(cfg);
		}

		{
			List<LTCFragment> newFragments = generateConfig(outputPath, nRecords, nLTCServers, nRangesPerServer);
			int newLTC = 0;
			for (int i = 0; i < newFragments.size(); i++) {
				LTCFragment frag = newFragments.get(i);
				if (frag.ltcServerId != nLTCServers - 1) {
					continue;
				}
				System.out.println(
						String.format("Migrate fragment %s from %d to %d", frag.toString(), frag.ltcServerId, newLTC));
				frag.ltcServerId = newLTC;
				newLTC = (newLTC + 1) % (nLTCServers - 1);
			}
			Configuration cfg = new Configuration();
			cfg.fragments = newFragments;
			for (int i = 0; i < nLTCServers - 1; i++) {
				cfg.ltcs.add(i);
			}
			for (int i = 0; i < nStoCs; i++) {
				cfg.stocs.add(nLTCServers + i);
			}
			configs.configs.add(cfg);
		}
		writeConfig(outputPath, arch, nRecords, nLTCServers, nStoCs, nRangesPerServer, 0, 1, configs);
	}

	private static void ComputeAddStoCConfig(String outputPath, String arch, int nRecords, int nLTCServers, int nStoCs,
			int nRangesPerServer) throws Exception, IOException {
		List<LTCFragment> fragments = generateConfig(outputPath, nRecords, nLTCServers, nRangesPerServer);
		Configurations configs = new Configurations();
		{
			Configuration cfg = new Configuration();
			cfg.fragments = fragments;
			for (int i = 0; i < nLTCServers; i++) {
				cfg.ltcs.add(i);
			}
			for (int i = 0; i < nStoCs - 1; i++) {
				cfg.stocs.add(nLTCServers + i);
			}
			configs.configs.add(cfg);
		}

		{
			List<LTCFragment> newFragments = generateConfig(outputPath, nRecords, nLTCServers, nRangesPerServer);
			Configuration cfg = new Configuration();
			cfg.fragments = newFragments;
			for (int i = 0; i < nLTCServers - 1; i++) {
				cfg.ltcs.add(i);
			}
			for (int i = 0; i < nStoCs; i++) {
				cfg.stocs.add(nLTCServers + i);
			}
			configs.configs.add(cfg);
		}
		writeConfig(outputPath, arch, nRecords, nLTCServers, nStoCs, nRangesPerServer, 0, 1, configs);
	}

	private static void ComputeRemoveStoCConfig(String outputPath, String arch, int nRecords, int nLTCServers,
			int nStoCs, int nRangesPerServer) throws Exception, IOException {
		List<LTCFragment> fragments = generateConfig(outputPath, nRecords, nLTCServers, nRangesPerServer);
		Configurations configs = new Configurations();
		{
			Configuration cfg = new Configuration();
			cfg.fragments = fragments;
			for (int i = 0; i < nLTCServers; i++) {
				cfg.ltcs.add(i);
			}
			for (int i = 0; i < nStoCs; i++) {
				cfg.stocs.add(nLTCServers + i);
			}
			configs.configs.add(cfg);
		}

		{
			List<LTCFragment> newFragments = generateConfig(outputPath, nRecords, nLTCServers, nRangesPerServer);
			Configuration cfg = new Configuration();
			cfg.fragments = newFragments;
			for (int i = 0; i < nLTCServers - 1; i++) {
				cfg.ltcs.add(i);
			}
			for (int i = 0; i < nStoCs - 1; i++) {
				cfg.stocs.add(nLTCServers + i);
			}
			configs.configs.add(cfg);
		}
		writeConfig(outputPath, arch, nRecords, nLTCServers, nStoCs, nRangesPerServer, 0, 1, configs);
	}

	private static void ComputeSimpleElasticConfig(String outputPath, String arch, int nRecords, int nLTCServers,
			int nStoCs, int nRangesPerServer) throws Exception, IOException {
		long now = 0;
		int initIntervalMin = 1;
		int intervalMin = 1;
		Configurations configs = new Configurations();
		{
			Configuration cfg = new Configuration();
			cfg.startTimeInSeconds = now;
			cfg.fragments = generateConfig(outputPath, nRecords, 1, nRangesPerServer);
			cfg.ltcs.add(0);
			cfg.stocs.add(2);
			configs.configs.add(cfg);
		}

		// Add one LTC. Move 50% fragment to LTC-1.
		{
			Configuration cfg = new Configuration();
			now += initIntervalMin * 60;
			cfg.startTimeInSeconds = now;
			cfg.fragments = generateConfig(outputPath, nRecords, 1, nRangesPerServer);
			for (int i = 0; i < cfg.fragments.size() / 2; i++) {
				cfg.fragments.get(i).ltcServerId = 1;
			}
			cfg.ltcs.add(0);
			cfg.ltcs.add(1);
			cfg.stocs.add(2);
			configs.configs.add(cfg);
		}

		// Add one StoC.
		for (int i = 1; i <= 1; i++) {
			now += intervalMin * 60;
			Configuration cfg = new Configuration();
			cfg.startTimeInSeconds = now;
			cfg.fragments = configs.configs.get(configs.configs.size() - 1).copy();
			cfg.ltcs.add(0);
			cfg.ltcs.add(1);
			cfg.stocs.add(2);
			cfg.stocs.add(3);
			configs.configs.add(cfg);
		}

		// Remove LTC-1. Move 100% fragments to LTC-0.
		{
			Configuration cfg = new Configuration();
			now += intervalMin * 60;
			cfg.startTimeInSeconds = now;
			cfg.fragments = configs.configs.get(configs.configs.size() - 1).copy();
			int moveToLTC = 0;
			for (int i = 0; i < cfg.fragments.size(); i++) {
				cfg.fragments.get(i).ltcServerId = moveToLTC;
			}
			cfg.ltcs.add(0);
			cfg.stocs.add(2);
			cfg.stocs.add(3);
			configs.configs.add(cfg);
		}

		// Remove one StoC.
		{
			now += intervalMin * 60;
			Configuration cfg = new Configuration();
			cfg.startTimeInSeconds = now;
			cfg.fragments = configs.configs.get(configs.configs.size() - 1).copy();
			cfg.ltcs.add(0);
			cfg.stocs.add(2);
			configs.configs.add(cfg);
		}
		writeConfig(outputPath, arch, nRecords, nLTCServers, nStoCs, nRangesPerServer, 0, 1, configs);
	}

	private static void ComputeElasticConfig(String outputPath, String arch, int nRecords, int nLTCServers, int nStoCs,
			int nRangesPerServer) throws Exception, IOException {
		long now = 0;
		int initIntervalMin = 10;
		int intervalMin = 5;
		Configurations configs = new Configurations();
		{
			Configuration cfg = new Configuration();
			cfg.startTimeInSeconds = now;
			cfg.fragments = generateConfig(outputPath, nRecords, 1, nRangesPerServer);
			for (int i = 0; i < 1; i++) {
				cfg.ltcs.add(i);
			}
			for (int i = 0; i < 1; i++) {
				cfg.stocs.add(nLTCServers + i);
			}
			configs.configs.add(cfg);
		}

		// Add one LTC. Move 50% fragment to LTC-1.
		{
			Configuration cfg = new Configuration();
			now += initIntervalMin * 60;
			cfg.startTimeInSeconds = now;
			cfg.fragments = generateConfig(outputPath, nRecords, 1, nRangesPerServer);
			for (int i = 0; i < cfg.fragments.size() / 2; i++) {
				cfg.fragments.get(i).ltcServerId = 1;
			}
			for (int i = 0; i < 2; i++) {
				cfg.ltcs.add(i);
			}
			for (int i = 0; i < 1; i++) {
				cfg.stocs.add(nLTCServers + i);
			}
			configs.configs.add(cfg);
		}

		// Add one LTC. Move 50% fragment from each LTC to LTC-2.
		{
			Configuration cfg = new Configuration();
			now += intervalMin * 60;
			cfg.startTimeInSeconds = now;
			cfg.fragments = configs.configs.get(configs.configs.size() - 1).copy();
			int fragPerServer = nRangesPerServer / 3 / 2;
			int[] moved = new int[2];

			for (int i = 0; i < cfg.fragments.size(); i++) {
				int serverid = cfg.fragments.get(i).ltcServerId;
				if (moved[serverid] == fragPerServer) {
					continue;
				}
				moved[serverid]++;
				cfg.fragments.get(i).ltcServerId = 2;
			}
			for (int i = 0; i < 3; i++) {
				cfg.ltcs.add(i);
			}
			for (int i = 0; i < 1; i++) {
				cfg.stocs.add(nLTCServers + i);
			}
			configs.configs.add(cfg);
		}

		// Add four StoC,
		for (int i = 1; i <= 4; i++) {
			now += intervalMin * 60;
			Configuration cfg = new Configuration();
			cfg.startTimeInSeconds = now;
			cfg.fragments = configs.configs.get(configs.configs.size() - 1).copy();
			for (int j = 0; j < 3; j++) {
				cfg.ltcs.add(j);
			}
			for (int j = 0; j < 1 + i; j++) {
				cfg.stocs.add(nLTCServers + j);
			}
			configs.configs.add(cfg);
		}

		// Remove LTC-2. Move 50% fragments to each LTC.
		{
			Configuration cfg = new Configuration();
			now += intervalMin * 60;
			cfg.startTimeInSeconds = now;
			cfg.fragments = configs.configs.get(configs.configs.size() - 1).copy();
			int rr = 2;
			int moveToLTC = 0;
			for (int i = 0; i < cfg.fragments.size(); i++) {
				int serverid = cfg.fragments.get(i).ltcServerId;
				if (serverid != 2) {
					continue;
				}
				cfg.fragments.get(i).ltcServerId = moveToLTC;
				moveToLTC = (moveToLTC + 1) % 2;
			}
			for (int i = 0; i < 2; i++) {
				cfg.ltcs.add(i);
			}
			for (int i = 0; i < 5; i++) {
				cfg.stocs.add(nLTCServers + i);
			}
			configs.configs.add(cfg);
		}

		// Remove LTC-1. Move 100% fragments to LTC-0.
		{
			Configuration cfg = new Configuration();
			now += intervalMin * 60;
			cfg.startTimeInSeconds = now;
			cfg.fragments = configs.configs.get(configs.configs.size() - 1).copy();
			int moveToLTC = 0;
			for (int i = 0; i < cfg.fragments.size(); i++) {
				int serverid = cfg.fragments.get(i).ltcServerId;
				if (serverid != 1) {
					continue;
				}
				cfg.fragments.get(i).ltcServerId = moveToLTC;
			}
			for (int i = 0; i < 1; i++) {
				cfg.ltcs.add(i);
			}
			for (int i = 0; i < 5; i++) {
				cfg.stocs.add(nLTCServers + i);
			}
			configs.configs.add(cfg);
		}

		// Remove four StoC,
		for (int i = 1; i <= 4; i++) {
			now += intervalMin * 60;
			Configuration cfg = new Configuration();
			cfg.startTimeInSeconds = now;
			cfg.fragments = configs.configs.get(configs.configs.size() - 1).copy();
			for (int j = 0; j < 1; j++) {
				cfg.ltcs.add(j);
			}
			for (int j = 0; j < 5 - i; j++) {
				cfg.stocs.add(nLTCServers + j);
			}
			configs.configs.add(cfg);
		}
		writeConfig(outputPath, arch, nRecords, nLTCServers, nStoCs, nRangesPerServer, 0, 1, configs);
	}

	private static void ComputeRW50Config(String outputPath, String arch, int nRecords, int nLTCServers, int nStoCs,
			int nRangesPerServer) throws Exception, IOException {
		long now = 0;
		int initIntervalMin = 20;
		int intervalMin = 15;
		Configurations configs = new Configurations();
		{
			Configuration cfg = new Configuration();
			cfg.startTimeInSeconds = now;
			cfg.fragments = generateConfig(outputPath, nRecords, 1, nRangesPerServer);
			for (int i = 0; i < 1; i++) {
				cfg.ltcs.add(i);
			}
			for (int i = 0; i < 1; i++) {
				cfg.stocs.add(nLTCServers + i);
			}
			configs.configs.add(cfg);
		}
		now += initIntervalMin * 60;
		// Add three StoC,
		for (int i = 1; i <= 3; i++) {
			now += intervalMin * 60;
			Configuration cfg = new Configuration();
			cfg.startTimeInSeconds = now;
			cfg.fragments = configs.configs.get(configs.configs.size() - 1).copy();
			for (int j = 0; j < 1; j++) {
				cfg.ltcs.add(j);
			}
			for (int j = 0; j < 1 + i; j++) {
				cfg.stocs.add(nLTCServers + j);
			}
			configs.configs.add(cfg);
		}
		// Remove three StoC,
		for (int i = 1; i <= 3; i++) {
			now += intervalMin * 60;
			Configuration cfg = new Configuration();
			cfg.startTimeInSeconds = now;
			cfg.fragments = configs.configs.get(configs.configs.size() - 1).copy();
			for (int j = 0; j < 1; j++) {
				cfg.ltcs.add(j);
			}
			for (int j = 0; j < 4 - i; j++) {
				cfg.stocs.add(nLTCServers + j);
			}
			configs.configs.add(cfg);
		}
		writeConfig(outputPath, arch, nRecords, nLTCServers, nStoCs, nRangesPerServer, 0, 1, configs);
	}

	private static void ComputeSW50Config(String outputPath, String arch, int nRecords, int nLTCServers, int nStoCs,
			int nRangesPerServer) throws Exception, IOException {
		long now = 0;
		int initIntervalMin = 10;
		int intervalMin = 15;
		Configurations configs = new Configurations();
		{
			Configuration cfg = new Configuration();
			cfg.startTimeInSeconds = now;
			cfg.fragments = generateConfig(outputPath, nRecords, 1, nRangesPerServer);
			for (int i = 0; i < 1; i++) {
				cfg.ltcs.add(i);
			}
			for (int i = 0; i < 10; i++) {
				cfg.stocs.add(nLTCServers + i);
			}
			configs.configs.add(cfg);
		}

		// Add one LTC. Move 50% fragment to LTC-1.
		{
			Configuration cfg = new Configuration();
			now += initIntervalMin * 60;
			cfg.startTimeInSeconds = now;
			cfg.fragments = generateConfig(outputPath, nRecords, 1, nRangesPerServer);
			for (int i = 0; i < cfg.fragments.size() / 2; i++) {
				cfg.fragments.get(i).ltcServerId = 1;
			}
			for (int i = 0; i < 2; i++) {
				cfg.ltcs.add(i);
			}
			for (int i = 0; i < 10; i++) {
				cfg.stocs.add(nLTCServers + i);
			}
			configs.configs.add(cfg);
		}

		// Add one LTC. Move 50% fragment from each LTC to LTC-2.
		{
			Configuration cfg = new Configuration();
			now += intervalMin * 60;
			cfg.startTimeInSeconds = now;
			cfg.fragments = configs.configs.get(configs.configs.size() - 1).copy();
			int fragPerServer = nRangesPerServer / 3 / 2;
			int[] moved = new int[2];

			for (int i = 0; i < cfg.fragments.size(); i++) {
				int serverid = cfg.fragments.get(i).ltcServerId;
				if (moved[serverid] == fragPerServer) {
					continue;
				}
				moved[serverid]++;
				cfg.fragments.get(i).ltcServerId = 2;
			}
			for (int i = 0; i < 3; i++) {
				cfg.ltcs.add(i);
			}
			for (int i = 0; i < 10; i++) {
				cfg.stocs.add(nLTCServers + i);
			}
			configs.configs.add(cfg);
		}

		now += intervalMin * 60;

		// Remove LTC-2. Move 50% fragments to each LTC.
		{
			Configuration cfg = new Configuration();
			now += intervalMin * 60;
			cfg.startTimeInSeconds = now;
			cfg.fragments = configs.configs.get(configs.configs.size() - 1).copy();
			int rr = 2;
			int moveToLTC = 0;
			for (int i = 0; i < cfg.fragments.size(); i++) {
				int serverid = cfg.fragments.get(i).ltcServerId;
				if (serverid != 2) {
					continue;
				}
				cfg.fragments.get(i).ltcServerId = moveToLTC;
				moveToLTC = (moveToLTC + 1) % 2;
			}
			for (int i = 0; i < 2; i++) {
				cfg.ltcs.add(i);
			}
			for (int i = 0; i < 10; i++) {
				cfg.stocs.add(nLTCServers + i);
			}
			configs.configs.add(cfg);
		}

		// Remove LTC-1. Move 100% fragments to LTC-0.
		{
			Configuration cfg = new Configuration();
			now += intervalMin * 60;
			cfg.startTimeInSeconds = now;
			cfg.fragments = configs.configs.get(configs.configs.size() - 1).copy();
			int moveToLTC = 0;
			for (int i = 0; i < cfg.fragments.size(); i++) {
				int serverid = cfg.fragments.get(i).ltcServerId;
				if (serverid != 1) {
					continue;
				}
				cfg.fragments.get(i).ltcServerId = moveToLTC;
			}
			for (int i = 0; i < 1; i++) {
				cfg.ltcs.add(i);
			}
			for (int i = 0; i < 10; i++) {
				cfg.stocs.add(nLTCServers + i);
			}
			configs.configs.add(cfg);
		}
		writeConfig(outputPath, arch, nRecords, nLTCServers, nStoCs, nRangesPerServer, 0, 1, configs);
	}
}
