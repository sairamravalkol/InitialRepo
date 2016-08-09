package tvg.hpl.hp.com.bean;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

/**
 * @author sarmaji
 *
 */
public class StartBfsBean {
	@NotNull
	@Pattern(regexp="(^[0-9]*$)" , message ="Start time must be a number")
	private String startTime;
	@NotNull
	@Pattern(regexp="(^[0-9]*$)" , message ="End time must be a number")
	private String endTime;
	@NotNull
	@Pattern(regexp="(^[0-9]*$)" , message ="hop must be a number")
	private String hop;
	@NotNull
	@Pattern(regexp="(^[0-9]{1,15}( *, *[0-9]{1,15})*$)" , message ="Vertices can be comma separated numbers")
	private String vertices;
	@NotNull
	@Pattern(regexp="(^[0-9]*$)" , message ="Push When done must be a number")
	private String push_when_done;
	

	public String getStartTime() {
		return startTime;
	}

	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	public String getEndTime() {
		return endTime;
	}

	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}

	public String getHop() {
		return hop;
	}

	public void setHop(String hop) {
		this.hop = hop;
	}

	public String getVertices() {
		return vertices;
	}

	public void setVertices(String vertices) {
		this.vertices = vertices;
	}

	public String getPushWhenDone() {
		return push_when_done;
	}

	public void setPushWhenDone(String push_when_done) {
		this.push_when_done = push_when_done;
	}
}
