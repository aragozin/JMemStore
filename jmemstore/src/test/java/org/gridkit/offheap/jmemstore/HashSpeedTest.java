/**
 *  Copyright 2011 Alexey Ragozin
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.gridkit.offheap.jmemstore;

import java.util.Random;

import org.junit.Ignore;

/**
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
@Ignore
public abstract class HashSpeedTest {

	private byte[][] dataset = new byte[64][];
	private int datasetSize = 0;
	private long start;
	private int n;
	private long bytesProcessed;
	private int hashsum;
	
	
	public void init() {
		Random rnd = new Random(0);
		for(int i = 0; i != dataset.length; ++i) {
			int len = rnd.nextInt(100000) + 10000;
			dataset[i] = new byte[len];
			rnd.nextBytes(dataset[i]);
			datasetSize += len;
		}
		
		for(int i = 0; i != 1000; ++i) {
			hash(dataset[i % dataset.length]);
		}
	}
	
	public void start() {
		n = 0;
		start = System.currentTimeMillis();
		bytesProcessed = 0;
		hashsum = 0;
		while(true) {
			iterate();
		}
	}

	private void iterate() {
		while(true) {
			hashsum += hash(dataset[n % dataset.length]);
			bytesProcessed += dataset[n % dataset.length].length;
			++n;
			if (n % 100000 == 0) {
				long now = System.currentTimeMillis();
				System.out.println("Processed " + (bytesProcessed >> 20) + "mb in " + ((now - start) / 1000) + "sec");
				System.out.println("Speed: " + ((bytesProcessed * 1000 / (now - start)) >> 20) + "mb/s");
				start = System.currentTimeMillis();
				bytesProcessed = 0;
				break;
			}
		}
	}
	
	public int hash(byte[] data) {
		return hash(data, 0, data.length);
	}
	
	public abstract int hash(byte[] data, int offs, int len);
}
