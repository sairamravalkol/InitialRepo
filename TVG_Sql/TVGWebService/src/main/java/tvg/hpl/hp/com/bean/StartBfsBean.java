package tvg.hpl.hp.com.bean;

/**
 * @author sarmaji
 *
 */
public class StartBfsBean {
	private String startTime;
	private String endTime;
	private String hop;
	private String vertices;
	private String push_when_done;
	private String taskId;
	private String Status;

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

	public String getPush_when_done() {
		return push_when_done;
	}

	public void setPush_when_done(String push_when_done) {
		this.push_when_done = push_when_done;
	}

	public String getTaskId() {
		return taskId;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	public String getStatus() {
		return Status;
	}

	public void setStatus(String status) {
		Status = status;
	}
	

}
