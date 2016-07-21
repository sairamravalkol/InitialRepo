package tvg.hpl.hp.com.ws;



public class TestResults {

	String vertices,starttime,endtime,hops;
	public String getVertices() {
		return vertices;
	}
	public void setVertices(String vertices) {
		this.vertices = vertices;
	}
	public String getStarttime() {
		return starttime;
	}
	public void setStarttime(String starttime) {
		this.starttime = starttime;
	}
	public String getEndtime() {
		return endtime;
	}
	public void setEndtime(String endtime) {
		this.endtime = endtime;
	}
	public String getHops() {
		return hops;
	}
	public void setHops(String hops) {
		this.hops = hops;
	}
	@Override
	public String toString() {
		return "Track [Vertices=" + vertices + ", StartTime=" + starttime + ", EndTime=" + endtime + ", Hops=" + hops + "]";
	}

}
