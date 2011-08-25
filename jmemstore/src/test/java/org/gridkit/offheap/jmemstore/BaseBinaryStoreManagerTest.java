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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import junit.framework.Assert;

import org.gridkit.offheap.jmemstore.BinaryKeyValueStore;
import org.gridkit.offheap.jmemstore.BinaryStoreManager;
import org.gridkit.offheap.jmemstore.ByteChunk;
import org.junit.Test;

/**
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public abstract class BaseBinaryStoreManagerTest {

	@Test
	public void test_basics() {
		
		BinaryStoreManager storeMan = createSmallPageManager();
		
		BinaryKeyValueStore store = storeMan.createKeyValueStore();
	
		{
			ByteChunk key = new ByteChunk("AAAA".getBytes());
			ByteChunk value = new ByteChunk("BBBB".getBytes());
			
			store.put(key, value);
			
			Assert.assertEquals(1, store.size());
			
			ByteChunk value2 = store.get(key);
			
			Assert.assertTrue(value.sameBytes(value2));
	
			ByteChunk value3 = new ByteChunk("CCCC".getBytes());
			store.put(key, value3);
			
			Assert.assertEquals(1, store.size());
			
			value2 = store.get(key);
			Assert.assertTrue(value3.sameBytes(value2));
			
			store.remove(key);
			
			Assert.assertEquals(0, store.size());
			Assert.assertTrue(store.get(key) == null);
		}
	
		{
			ByteChunk key1 = new ByteChunk(new byte[8]); // this way we can fabricate same hash codes
			ByteChunk key2 = new ByteChunk(new byte[9]); // this way we can fabricate same hash codes
			key1.putInt(0, -1);
			key1.putInt(4, 100);
			key2.putInt(0, -1);
			key2.set(4, (byte) 0x00);
			key2.putInt(5, 100);
			
			store.put(key1, key1);
			store.put(key2, key2);
			Assert.assertEquals(2, store.size());
			
			ByteChunk val;
			val = store.get(key1);
			Assert.assertTrue(key1.sameBytes(val));
			
			val = store.get(key2);
			Assert.assertTrue(key2.sameBytes(val));
			
			store.remove(key1);
			Assert.assertEquals(1, store.size());
			Assert.assertTrue(store.get(key1) == null);

			val = store.get(key2);
			Assert.assertTrue(key2.sameBytes(val));
			
			store.clear();
			
			Assert.assertEquals(0, store.size());
			Assert.assertNull(store.get(key1));
			Assert.assertNull(store.get(key2));
		}
		
		storeMan.close();
	}

//	protected BinaryStoreManager createSmallPageManager() {
//		PageLogManager pageManager = new PageLogManager(8 << 10, 16, 2);
//		
//		PagedMemoryBinaryStoreManager storeMan = new PagedMemoryBinaryStoreManager("test_basics", pageManager);
//		return storeMan;
//	}

	protected abstract BinaryStoreManager createSmallPageManager();
	
	@Test
	public void large_consistency_test_basics() {
		
		BinaryStoreManager storeMan = createLargeStoreManager();
		
		BinaryKeyValueStore store = storeMan.createKeyValueStore();
		
		Random rnd = new Random(1);
		Map<String, String> refMap = new HashMap<String, String>();
		
		int objNum = 10000;
		int holeNum = 2000;
		
		for(int n = 0; n != 100000; ++n) {

			
			if (n > 85000 && objNum > 0) {
				--objNum;
				++holeNum;
			}
			
			if (n % 500 == 0) {
				compare(refMap, store, objNum, holeNum);
			}
			
			
			int size = refMap.size(); 
			if (size > objNum) {
 				if (n > 85000 || ((size - objNum) >= rnd.nextInt(holeNum))) {
					while(true) {
						String key;
						if (size < (objNum + holeNum ) / 8) {
							key = refMap.keySet().iterator().next();
						}
						else{							
							key = randomKey(rnd, objNum, holeNum);
						}
						boolean hit = refMap.remove(key) != null;
//						System.out.println("remove(" + key + ")");
						store.remove(toByteChunk(key));
						Assert.assertEquals(refMap.size(), store.size());
						if (hit) {
							break;
						}
					}
					continue;
				}
			}

			String key = randomKey(rnd, objNum, holeNum);
			String val = randomString(rnd.nextInt(10) + 20, rnd);
			
			if (refMap.containsKey(key)) {
//				System.out.println("insert(" + key + ", " + val + "), size=" + refMap.size());
			}
			else {
//				System.out.println("update(" + key + ", " + val + "), size=" + refMap.size());
			}
			if (key.equals("108071")) {
				new String();
			}
			if (refMap.containsKey("109497")) {
				ByteChunk bc;
				if ((bc = store.get(toByteChunk("109497"))) == null) {
					Assert.assertFalse(true);
				}
				store.put(toByteChunk("109497"), bc);
			}
			refMap.put(key, val);
			if (n == 26) {
				new String();
			}			
			store.put(toByteChunk(key), toByteChunk(val));
			if (refMap.size() != store.size()) {
				Assert.assertEquals(refMap.size(), store.size());
			}
			ByteChunk bval = store.get(toByteChunk(key));
			Assert.assertTrue(toByteChunk(val).sameBytes(bval));
			
			if (refMap.containsKey("109497")) {
				if (store.get(toByteChunk("109497")) == null) {
					Assert.assertFalse(true);
				}
			}
			
			if (n == 90000) {
				// test clear correctness
				store.clear();
				refMap.clear();
			}			
		}
		
		compare(refMap, store, objNum, holeNum);
		
		storeMan.close();
	}

//	protected BinaryStoreManager createLargeStoreManager() {
//		PageLogManager pageManager = new PageLogManager(64 << 10, 256, 2);
//		
//		PagedMemoryBinaryStoreManager storeMan = new PagedMemoryBinaryStoreManager("test_basics", pageManager);
//		return storeMan;
//	}

	abstract protected BinaryStoreManager createLargeStoreManager();

	private static void compare(Map<String, String> ref, BinaryKeyValueStore store, int objNum, int holeNum) {
		for(int i = 0; i != objNum + holeNum; ++i) {
			String key = String.valueOf(100000l + i);
			
			String val = ref.get(key);
			ByteChunk bval = store.get(toByteChunk(key));
			
			if (val == null) {
				Assert.assertTrue(bval == null);
			}
			else {
				if (bval == null) {
					System.out.println("Mismatch: Missing key " + key);
					Assert.assertFalse(true);
				}
				Assert.assertTrue(bval.sameBytes(toByteChunk(val)));
			}
		}
	}

	private static ByteChunk toByteChunk(String val) {
		return new ByteChunk(val.getBytes());
	}
	
	private static String randomKey(Random rnd, int objNum, int holeNum) {
		long key = 100000 + rnd.nextInt(objNum + holeNum);
		return String.valueOf(key);
	}
	
	static char[] CHARS_BUFFER = new char[1024];
	public static String randomString(int len, Random rnd) {
		if (len > 1024 || len < 0) {
			throw new IllegalArgumentException("String length exceeds buffer size");
		}
		for(int i = 0; i != len; ++i) {
			CHARS_BUFFER[i] = (char)('A' + rnd.nextInt(23));
		}
		return new String(CHARS_BUFFER, 0, len);
	}
}
