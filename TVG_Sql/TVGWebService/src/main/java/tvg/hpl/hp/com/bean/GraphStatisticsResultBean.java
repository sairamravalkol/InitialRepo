package tvg.hpl.hp.com.bean;

public class GraphStatisticsResultBean {
	private Integer num_vertices;
	private Integer num_edges;
		
	public GraphStatisticsResultBean() {
		
	}
	public Integer getNum_vertices() {
		return num_vertices;
	}
	public void setNum_vertices(Integer num_vertices) {
		this.num_vertices = num_vertices;
	}
	public Integer getNum_edges() {
		return num_edges;
	}
	public void setNum_edges(Integer num_edges) {
		this.num_edges = num_edges;
	}
	@Override
	public String toString() {
		return "StatisticsBean [num_vertices=" + num_vertices + ", num_edges=" + num_edges + "]";
	}
	
	

}
