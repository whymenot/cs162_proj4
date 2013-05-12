package edu.berkeley.cs162;

import static org.junit.Assert.*;

import org.junit.Test;

public class KVCache_test {

	@Test
	public void KVCacheTest() {
		KVCache cache = new KVCache(5,5);
		cache.put("k1", "v1");
		assertTrue(cache.get("k1").equals("v1"));
		String s = "k1";
		String[] strs ={"a","k","a2","f","k2","f2","q","q2","e","m","h","i"};
		
		for(int i = 0; i<strs.length; i++){
			int num = Math.abs(strs[i].hashCode()) % 5;
			if(i==3){
				assertTrue(cache.get("a").equals("a"));
			}
			cache.put(strs[i], strs[i]);
			System.out.println(num);
		}
		assertTrue(cache.get("k")==null);
		try {
			System.out.println("cache is: " +cache.toXML());
		} catch (KVException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		for(int i = 0; i<strs.length; i++){
			if(strs[i]!="k"){
				assertTrue(cache.get(strs[i])==strs[i]);
				cache.del(strs[i]);
			}
			
		}
		for(int i = 0; i<strs.length; i++){
			assertTrue(cache.get(strs[i])==null);
		}
		
		//Test for XML
		KVCache cache2 = new KVCache(2,2);
		cache2.put("k1", "v1");
		cache2.put("k2", "v2");
		String expectedXML = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><KVCache><Set Id=\"0\"><KVPair><Key>k1</Key><Value>v1</Value></KVPair></Set><Set Id=\"1\"><KVPair><Key>k2</Key><Value>v2</Value></KVPair></Set></KVCache>";
		String realXML = "";
		try {
			realXML = cache2.toXML();
		} catch (KVException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assertTrue(expectedXML.compareTo(realXML)==0);

	}

}
