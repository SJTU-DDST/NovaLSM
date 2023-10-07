package com.yahoo.ycsb.db;

import java.util.List;

import com.google.common.collect.Lists;

public class LTCFragment {
	public long startKey;
	public long endKey;
	public int ltcServerId;
	public int dbId;
	public double refCount = 0;

	public List<Integer> stocLogReplicaServerIds = Lists.newArrayList();

	public LTCFragment copy() {
		LTCFragment frag = new LTCFragment();
		frag.startKey = startKey;
		frag.endKey = endKey;
		frag.ltcServerId = ltcServerId;
		frag.dbId = dbId;
		frag.refCount = refCount;
		return frag;
	}

	@Override
	public String toString() {
		return "Fragment [start=" + startKey + ", end=" + endKey + ", serverIds=" + ltcServerId + ", dcServerIds="
				+ stocLogReplicaServerIds + "]";
	}

	public String toConfigString() {
		StringBuilder builder = new StringBuilder();
		builder.append(startKey);
		builder.append(",");
		builder.append(endKey);
		builder.append(",");
		builder.append(ltcServerId);
		builder.append(",");
		builder.append(dbId);
		builder.append(",");
		for (int i = 0; i < stocLogReplicaServerIds.size(); i++) {
			builder.append(stocLogReplicaServerIds.get(i));
			builder.append(",");
		}
		builder.deleteCharAt(builder.length() - 1);
		return builder.toString();
	}

}
