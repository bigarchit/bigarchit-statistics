package com.bigarchit.statistics.dump;

public class KVPair<Key, Value> {
	public Key key;
	public Value value;

	public KVPair(Key key, Value value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public String toString() {
		return "KVPair [key=" + key + ", value=" + value + "]";
	}

}
