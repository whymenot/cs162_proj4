package edu.berkeley.cs162;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;

public class KVStore_test {

	@Test
	public void KVStoreTest() throws KVException {
		//Test for toXML
		KVStore store = new KVStore();
		store.put("k1", "v1");
		//assertFalse(store.put("k1", "v1"));
		assertTrue(store.get("k1").equals("v1"));
		store.put("k2", "v2");
		String expectedXML = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><KVStore><KVPair><Key>k2</Key><Value>v2</Value></KVPair><KVPair><Key>k1</Key><Value>v1</Value></KVPair></KVStore>";
		String realXML = store.toXML();
		assertTrue(expectedXML.compareTo(realXML)==0);
		//Test for dumpToFile
		store.dumpToFile("test1.xml");
		
		//Test for restoreFromFile
		KVStore store2 = new KVStore();
		try {
			store2.restoreFromFile("test1.xml");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			File file = new File("test1.xml");
			file.delete();
		}
		realXML = store2.toXML();
		assertTrue(expectedXML.compareTo(realXML)==0);
	}

}

/*

<?xml version="1.0" encoding="UTF-8" standalone="no"?><KVStore><KVPair><Key>k2</Key><Value>v2</Value></KVPair><KVPair><Key>k1</Key><Value>v1</Value></KVPair></KVStore>
<?xml version="1.0" encoding="UTF-8" standalone="no"?><KVStore><KVPair><Key>k2</Key><Value>v2</Value></KVPair><KVPair><Key>k1</Key><Value>v1</Value></KVPair></KVStore>

*/
