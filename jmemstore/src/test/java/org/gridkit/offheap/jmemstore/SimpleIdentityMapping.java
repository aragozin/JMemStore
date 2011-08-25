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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.gridkit.offheap.jmemstore.ByteChunk;
import org.gridkit.offheap.jmemstore.IdentityMapping;
import org.junit.Assert;
import org.junit.Ignore;

/**
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
@Ignore
public class SimpleIdentityMapping {

	private Map<String, Integer> s2i = new HashMap<String, Integer>();
	private Map<Integer, String> i2s = new HashMap<Integer, String>();
	private Map<Integer, Integer> refCount = new HashMap<Integer, Integer>();
	
	public ByteChunk getChunkById(int id) {		
		return toChunk(i2s.get(id));
	}
	
	public int getIdByChunk(ByteChunk chunk) {
		Integer x = s2i.get(toString(chunk));
		return x == null ? IdentityMapping.UNMAPPED : x;
	}
	
	public void map(ByteChunk chunk, int id) {
		String s = toString(chunk);
		Integer ref = refCount.get(id);
		if (ref == null) {
			refCount.put(id, 1);
		}
		else {
			if (!(s.equals(i2s.get(id))) || id != s2i.get(s)) {
				Assert.assertEquals(s, i2s.get(id));
				Assert.assertEquals(Integer.valueOf(id), s2i.get(s));
			}
			refCount.put(id, ref + 1);
		}
		
		s2i.put(s, id);
		i2s.put(id, s);
		
		if (s2i.size() != i2s.size()) {
			Assert.assertFalse(true);
		}
	}
	
	public int size() {
		return s2i.size();
	}

	public void unmap(int id) {
		String s = i2s.get(id);
		Integer ref = refCount.get(id);
		if (ref.intValue() == 1) {
			refCount.remove(id);
			i2s.remove(id);
			s2i.remove(s);
		}
		else {
			refCount.put(id, ref - 1);
		}
		
		if (s2i.size() != i2s.size()) {
			Assert.assertFalse(true);
		}
	}

	public void unmap(ByteChunk chunk) {
		int id = s2i.get(toString(chunk));
		Integer ref = refCount.get(id);
		if (ref.intValue() == 1) {
			refCount.remove(id);
			i2s.remove(id);
			s2i.remove(toString(chunk));
		}
		else {
			refCount.put(id, ref - 1);
		}
		
		if (s2i.size() != i2s.size()) {
			Assert.assertFalse(true);
		}
	}
	
	public int cardinality(int id) {
		Integer x = refCount.get(id);
		return x == null ? 0 : x;
	}

	public int cardinality(ByteChunk chunk) {
		Integer x = refCount.get(getIdByChunk(chunk));
		return x == null ? 0 : x;
	}
	
	public int getValidId() {
		if (!i2s.isEmpty()) {
			return i2s.keySet().iterator().next();
		}
		else {
			return IdentityMapping.UNMAPPED;
		}
	}
	
	public Collection<String> keySet() {
		return s2i.keySet();
	}
	
	private ByteChunk toChunk(String text) {
		if (text == null) {
			return null;
		}
		int len = text.length() / 2;
		byte[] bytes = new byte[len];
		for(int i = 0; i != len; ++i) {
			int val = Character.digit(text.charAt(2 * i), 16) << 4;
			val += Character.digit(text.charAt(2 * i + 1), 16);
			bytes[i] = (byte) val;
		}
		return new ByteChunk(bytes);
	}
	
	private static char[] HEX = {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
	
	private String toString(ByteChunk chunk) {
		if (chunk == null) {
			return null;
		}
		StringBuffer buf = new StringBuffer();
		for(int i = 0; i != chunk.lenght(); ++i) {
			int val = chunk.at(i);
			buf.append(HEX[(val & 0xFF) >> 4]);
			buf.append(HEX[val & 0xF]);
		}
		return buf.toString();
	}
}
