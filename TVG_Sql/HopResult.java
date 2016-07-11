package com.hpl.hp.com.dao;

public final class HopResult {

	private final long edge_count;
	private final long time_taken;
	
	public long getEdge_count() {
		return edge_count;
	}
	public long getTime_taken() {
		return time_taken;
	}
	public HopResult(long edge_countpara, long timetakenpara) {
		// TODO Auto-generated constructor stub
		this.edge_count=edge_countpara;
		this.time_taken=timetakenpara;
		
	}

}
