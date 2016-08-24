package tvg.hpl.hp.com.validation;

/**
 * @author sarmaji
 *
 */

import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import tvg.hpl.hp.com.bean.GetGraphBean;
import tvg.hpl.hp.com.util.ApplicationConstants;

public class GetGraphBeanValidation extends Exception {
	private static final long serialVersionUID = 1L;

	public GetGraphBeanValidation(Set<ConstraintViolation<GetGraphBean>> constraintViolations) {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode objectNode = mapper.createObjectNode();
		objectNode.put("status code",  ApplicationConstants.RESPONSE_CODE_BAD_REQUEST);
		for (ConstraintViolation<GetGraphBean> cons : constraintViolations) {
			if (cons.getPropertyPath().toString().equals(ApplicationConstants.TASKID))
				objectNode.put(cons.getPropertyPath().toString(), cons.getMessage());
			}
		String jsonErrorString = objectNode.toString();
		throw new WebApplicationException(
				Response.status( ApplicationConstants.RESPONSE_CODE_BAD_REQUEST).entity(jsonErrorString).type(MediaType.APPLICATION_JSON).build());
	}
}
