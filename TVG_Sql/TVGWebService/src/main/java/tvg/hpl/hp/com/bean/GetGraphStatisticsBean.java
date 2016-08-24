/**
 * 
 */
package tvg.hpl.hp.com.bean;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

/**
 * @author sarmaji
 *
 */
public class GetGraphStatisticsBean {
	@NotNull
	@Pattern(regexp="(^[0-9]*$)" , message ="Task Id must be a number")
	private String taskId;

	public String getTaskId() {
		return taskId;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

}
