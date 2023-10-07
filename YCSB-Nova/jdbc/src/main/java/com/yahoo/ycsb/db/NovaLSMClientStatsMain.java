package com.yahoo.ycsb.db;

import java.util.List;

import com.google.common.collect.Lists;

public class NovaLSMClientStatsMain {

	public static void main(String[] args) {
		List<String> servers = Lists.newArrayList(args[0].split(","));
		if (args.length == 1) {
			queryDBStats(servers);
		} else if (args.length == 2) {
			System.out.println("Drain Database.");
			NovaClient client = new NovaClient(servers, true);
			for (int i = 0; i < servers.size(); i++) {
				client.drainDB(i);
			}
		}
	}

	public static void queryDBStats(List<String> servers) {
		NovaClient client = new NovaClient(servers, true);
		while (true) {
			boolean stop = true;
			for (int i = 0; i < servers.size(); i++) {
				int nl0tables = client.stats(i);
				System.out.println(String.format("Waiting for L0 tables %d on %d", nl0tables, i));
				if (nl0tables > 0) {
					stop = false;
				}
			}
			if (!stop) {
				try {
					Thread.sleep(1000 * 10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {
				break;
			}
		}
	}

}
