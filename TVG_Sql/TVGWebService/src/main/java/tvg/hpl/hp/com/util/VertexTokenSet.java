package tvg.hpl.hp.com.util;
/**
 * 
 */

import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class VertexTokenSet {
	Set<String>vertexSet = new HashSet<String>();
	String vertex;
	
	public Set<String> getVerxtexSet(String vertices){
		StringTokenizer token = new StringTokenizer(vertices, ",");
		while(token.hasMoreTokens()){
			vertex = token.nextToken();
			vertexSet.add(vertex);
		}
		System.out.println("Vertex set :"+vertexSet);
		return vertexSet;
	}
}
