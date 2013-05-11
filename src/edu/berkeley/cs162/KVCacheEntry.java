package edu.berkeley.cs162;

public class KVCacheEntry {

	private boolean refBit;
	private String data;

	public KVCacheEntry(String data){
		this.data = data;
		this.refBit = false;
	}
	public void setData(String val){ this.data = val;}
	public void setRefBit(boolean val){ this.refBit = val; }
	public String getData(){ return this.data; }
	public boolean getRefBit(){ return this.refBit; }
}
