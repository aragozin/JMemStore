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

import org.gridkit.offheap.jmemstore.BinHash;
import org.gridkit.offheap.jmemstore.ByteChunk;
import org.junit.Ignore;

/**
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
@Ignore
public class CRC32SpeedTest {

	public static void main(String[] args) {
		HashSpeedTest test = new HashSpeedTest() {
			@Override
			public int hash(byte[] data, int offs, int len) {
				return BinHash.hash(new ByteChunk(data, offs, len));
			}
		};
		
		test.init();
		test.start();
	}
	
}
