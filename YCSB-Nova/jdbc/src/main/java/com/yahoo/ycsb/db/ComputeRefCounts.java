package com.yahoo.ycsb.db;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Random;

import com.yahoo.ycsb.generator.ZipfianGenerator;

public class ComputeRefCounts {

	public static class Record {
		int recordId = 0;
		int tenantId = 0;
		int refCount = 0;
		double accessFreq = 0;

		public Record() {

		}

		public Record(int recordId, int tenantId, int refCount, double accessFreq) {
			super();
			this.recordId = recordId;
			this.tenantId = tenantId;
			this.refCount = refCount;
			this.accessFreq = accessFreq;
		}
	}

	public static void main(String[] args) throws Exception {
		int nrecords = Integer.parseInt(args[0]);
		double zipfianconstant = Double.parseDouble(args[1]);
		int nrecordsPerRead = Integer.parseInt(args[2]);
		computeRefCount(nrecords, zipfianconstant, nrecordsPerRead);
	}

	public static void computeRefCounts(double zipfConstant) throws Exception {
		System.out.println("Monte Carlo simulation on constant " + zipfConstant);
		int maxLoop = 100000000;
		int nrecords = 10000000;
		Random rand = new Random();
		int first_range = nrecords / 10 / 64;
		int first_server = nrecords / 10;
		int refs_first_range = 0;
		int refs_first_server = 0;

		ZipfianGenerator zipf = new ZipfianGenerator(nrecords, zipfConstant);
		for (int i = 0; i < maxLoop; i++) {
			int key = zipf.nextValue().intValue();
			if (key < first_range) {
				refs_first_range++;
			}
			if (key < first_server) {
				refs_first_server++;
			}
		}
		System.out.println(String.format("%f:%f", (double) refs_first_range / (double) maxLoop,
				(double) refs_first_server / (double) maxLoop));

	}

	public static void computeRefCount(int nrecords, double zipfConstant, int nrecordsPerRead) throws Exception {
		System.out.println("Monte Carlo simulation on constant " + zipfConstant);
		int maxLoop = nrecords * 10;
		int[] refCount = new int[nrecords];
		ZipfianGenerator zipf = new ZipfianGenerator(nrecords, zipfConstant);
		for (int i = 0; i < maxLoop; i++) {
			int key = zipf.nextValue().intValue();
			if (i % 2 == 0) {
				refCount[key] += 1;
			} else {
				for (int j = 0; j < nrecordsPerRead; j++) {
					if (key + j < nrecords) {
						refCount[key + j] += 1;
					}
				}
			}
		}
		BufferedWriter bw = new BufferedWriter(
				new FileWriter(new File(String.format("/tmp/zipfian-%d-%.2f", nrecords, zipfConstant))));
		for (int i = 0; i < refCount.length; i++) {
			bw.write(String.format("%f\n", (double) refCount[i]));
		}
		bw.close();
	}
}