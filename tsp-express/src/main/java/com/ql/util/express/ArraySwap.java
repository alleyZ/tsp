package com.ql.util.express;

import java.io.Serializable;

public final class ArraySwap implements Serializable {
	OperateData[] arrays;
	int start;
	public int length;
	
	public void swap(OperateData[] aArrays,int aStart ,int aLength){
		this.arrays = aArrays;
		this.start = aStart;
		this.length = aLength;
	}
	public OperateData get(int i){
		return this.arrays[i+start];
	}

}
