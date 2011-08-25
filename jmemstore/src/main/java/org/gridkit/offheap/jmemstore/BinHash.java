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

/**
 * A simple variation of CRC
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
class BinHash {

	static public int[] CRC32_TABLE = new int[256];
	
	static
	{
		for (int i = 0; i < 256; i++)
		{
			int r = i;
			for (int j = 0; j < 8; j++)
				if ((r & 1) != 0)
					r = (r >>> 1) ^ 0xEDB88320;
				else
					r >>>= 1;
			CRC32_TABLE[i] = r;
		}
	}

	
	public static int hash(int n) {
		return appendHash(-1, n);
	}
	
	public static int hash(ByteChunk bytes) {
		return appendHash(-1, bytes);
	}
	
	public static int appendHash(int hash, int n) {
		hash = CRC32_TABLE[(hash ^ (n & 0xFF)) & 0xFF] ^ (hash >>> 8);
		hash = CRC32_TABLE[(hash ^ ((n >> 8) & 0xFF)) & 0xFF] ^ (hash >>> 8);
		hash = CRC32_TABLE[(hash ^ ((n >> 16) & 0xFF)) & 0xFF] ^ (hash >>> 8);
		hash = CRC32_TABLE[(hash ^ ((n >> 24) & 0xFF)) & 0xFF] ^ (hash >>> 8);
		return hash;
	}
	
	public static int appendHash(int hash, ByteChunk bytes)	{
		for (int i = 0; i < bytes.lenght(); i++) {
			hash = CRC32_TABLE[(hash ^ bytes.at(i)) & 0xFF] ^ (hash >>> 8);
		}
		return hash;
	}

	public static int splitHash(int hash, int capacity) {
	    int round = Integer.highestOneBit(capacity);
	    int split = capacity & ~round;
	
	    long idx = (0xFFFFFFFFl & hash) % (round);
	    
	    if (idx < split) {
	    	idx = (0xFFFFFFFFl & hash) % (round << 1);
	    }
	    return (int) idx;
	}
	
	public static int murmur3_fmix(int h) {
		h ^= h >> 16;
	    h *= 0x85ebca6d;
	    h ^= h >> 13;
	    h *= 0xc2b2ae35;
	    h ^= h >> 16;
	    
	    return h;
	}
	
	public static int murmur3_mix(byte[] data, int offs, int len, int seed) {
		
		int h1 = seed;
		int c1 = 0xcc9e2d51;
		int c2 = 0x1b873593;
		int k1;
		
		int p = offs;
		int l = offs + len;
		while(p + 3 < l) {
			int block = data[p++];
			block |= (0xFF & data[p++]) << 8;
			block |= (0xFF & data[p++]) << 16;
			block |= (0xFF & data[p++]) << 24;
			
			k1 = block;
		    k1 *= c1;
		    k1 = Integer.rotateLeft(k1, 15);
		    k1 *= c2;
		    
		    h1 ^= k1;
		    h1 = Integer.rotateLeft(h1, 13);
		    h1 = h1*5 +0xe6546b64;
		}
		
		int block = 0;
		switch(l - p) {
			case 3: block |= (0xFF & data[p+2]) << 16;
			case 2: block |= (0xFF & data[p+1]) << 8;
			case 1: block |= (0xFF & data[p]);
		}
		k1 = block;
		k1 *= c1;
		k1 = Integer.rotateLeft(k1, 15);
		k1 *= c2;
		h1 ^= k1;
		
		return h1;
	}
	
	public static int murmur3_hash(byte[] data, int offs, int len, int seed) {
		int h = murmur3_mix(data, offs, len, seed);
		h ^= len;
		return murmur3_fmix(h);
	}
}
