package tvg.hpl.hp.com.graph.json;

/**
 * @author sarmaji
 *
 */

public class Vertices {
	private String machine_type;
	private String page_rank;
	private String black_list;
	private String external_flag;
	private String _id;
	private String _type;
	
	public String get_id() {
		return _id;
	}
	public void set_id(String _id) {
		this._id = _id;
	}
	public String get_type() {
		return _type;
	}
	public void set_type(String _type) {
		this._type = _type;
	}
	public String getMachine_type() {
		return machine_type;
	}
	public void setMachine_type(String machine_type) {
		this.machine_type = machine_type;
	}
	public String getPage_rank() {
		return page_rank;
	}
	public void setPage_rank(String page_rank) {
		this.page_rank = page_rank;
	}
	public String getBlack_list() {
		return black_list;
	}
	public void setBlack_list(String black_list) {
		this.black_list = black_list;
	}
	public String getExternal_flag() {
		return external_flag;
	}
	public void setExternal_flag(String external_flag) {
		this.external_flag = external_flag;
	}
	

}
