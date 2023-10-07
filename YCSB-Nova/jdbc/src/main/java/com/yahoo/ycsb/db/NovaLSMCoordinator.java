package com.yahoo.ycsb.db;

import java.util.List;

import com.google.common.collect.Lists;
import com.yahoo.ycsb.db.ConfigurationUtil.Configurations;

public class NovaLSMCoordinator {

	public static void main(String[] args) throws Exception {
		List<String> servers = Lists.newArrayList(args[0].split(","));
		String configFile = args[1];
		Configurations configs = ConfigurationUtil.readConfig(configFile);
		NovaClient client = new NovaClient(servers, true);

		int now = 0;
		for (int cfgId = 1; cfgId < configs.configs.size(); cfgId++) {
			long startTime = configs.configs.get(cfgId).startTimeInSeconds;
			while (true) {
				if (now == startTime) {
					break;
				}
				Thread.sleep(1000);
				now++;
			}

			for (int i = servers.size() - 1; i >= 0; i--) {
				client.changeConfig(i);
				System.out.println("Change config for server " + i);
			}

			long start = System.currentTimeMillis();
			while (true) {
				boolean isAllReady = true;
				for (int i = 0; i < servers.size(); i++) {
					boolean isReady = client.queryConfigComplete(i);
					if (isReady) {
						System.out.println("Server " + i + " is ready");
					} else {
						isAllReady = false;
						System.out.println("Server " + i + " is not ready");
					}
				}
				if (isAllReady) {
					break;
				}
				Thread.sleep(10);
			}

			long end = System.currentTimeMillis();
			long duration = end - start;
			System.out.println(cfgId + " Take to complete configuration change " + duration);
		}
	}
}
