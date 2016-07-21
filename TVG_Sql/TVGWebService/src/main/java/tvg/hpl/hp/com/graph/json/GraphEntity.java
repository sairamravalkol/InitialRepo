package tvg.hpl.hp.com.graph.json;

/**
 * @author sarmaji
 *
 */
import java.util.List;

public class GraphEntity {
	private String mode;
	List<Vertices> vertices;
	List<Edges> edges;	
	
	public List<Vertices> getVertices() {
		return vertices;
	}
	public void setVertices(List<Vertices> vertices) {
		this.vertices = vertices;
	}
	public List<Edges> getEdges() {
		return edges;
	}
	public void setEdges(List<Edges> edges) {
		this.edges = edges;
	}
	public String getMode() {
		return mode;
	}
	public void setMode(String mode) {
		this.mode = mode;
	}
	
}
