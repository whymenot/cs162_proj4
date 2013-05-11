package edu.berkeley.cs162;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
public class KVCacheSet{

	private int maxSize;
	public Map <String, KVCacheEntry> entries;
	private LinkedList <String> queue;
	private ReentrantReadWriteLock lock;

	public KVCacheSet(int size){
		this.maxSize = size;
		this.entries = new HashMap<String, KVCacheEntry>();
		this.queue = new LinkedList<String>();
		this.lock = new ReentrantReadWriteLock();
	}



	public boolean put (String key, String value){
		if (entries.containsKey(key)) {
			if (entries.get(key).getData() == value){
				return false;//may need to update ref bit here?
			}
			else{
				KVCacheEntry curr = entries.get(key);
				curr.setData(value);
				return true;
			}
		}
		else if(this.maxSize == entries.size()){//the set is full
			//evict a key based on second-chance replacement policy
			String toRemove = queue.removeFirst();
			while(entries.get(toRemove).getRefBit() == true){
				queue.add(toRemove);
				entries.get(toRemove).setRefBit(false);
				toRemove = queue.removeFirst();
			}
			entries.remove(toRemove);
			entries.put(key, new KVCacheEntry(value));
			return true;//not sure if eviction counts as override
		}
		else{
			queue.add(key);
			entries.put(key, new KVCacheEntry(value));
			return false;
		}
	}

    public String get(String key){
			if(entries.containsKey(key)){
				KVCacheEntry entry = entries.get(key);
				entry.setRefBit(true);
				return entry.getData();
			}
			else{
				return null;
			}
	}

	public void del(String key){
		entries.remove(key);
		queue.remove(key);
	}
	
	public WriteLock getWriteLock(){
		return this.lock.writeLock();
	}
}