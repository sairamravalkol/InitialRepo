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

import tvg.hpl.hp.com.bean.StartBfsBean;
import tvg.hpl.hp.com.util.ApplicationConstants;
/**
 * StartBfsBeanValidation validates all the input parameter to the startbfs web service. It checks the input
 * parameters satisfies all the rule defined otherwise send a BAD_REQUEST(response status code 400) specifying the 
 * error to the client.
 * @author sarmaji
 *
 */
public class StartBfsBeanValidation extends Exception {
	private static final long serialVersionUID = 1L;

	public StartBfsBeanValidation(Set<ConstraintViolation<StartBfsBean>> constraintViolations) {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode objectNode = mapper.createObjectNode();
		objectNode.put("status code", ApplicationConstants.RESPONSE_CODE_BAD_REQUEST);
		for (ConstraintViolation<StartBfsBean> cons : constraintViolations) {
			if (cons.getPropertyPath().toString().equals(ApplicationConstants.STARTBFS_START_TIME))
				objectNode.put(cons.getPropertyPath().toString(), cons.getMessage());
			else if (cons.getPropertyPath().toString().equals(ApplicationConstants.STARTBFS_END_TIME))
				objectNode.put(cons.getPropertyPath().toString(), cons.getMessage());
			else if (cons.getPropertyPath().toString().equals(ApplicationConstants.STARTBFS_HOP))
				objectNode.put(cons.getPropertyPath().toString(), cons.getMessage());
			else if (cons.getPropertyPath().toString().equals(ApplicationConstants.STARTBFS_VERTICES))
				objectNode.put(cons.getPropertyPath().toString(), cons.getMessage());
			else if (cons.getPropertyPath().toString().equals(ApplicationConstants.STARTBFS_PUSH_WHEN_DONE))
				objectNode.put(cons.getPropertyPath().toString(), cons.getMessage());
		}
		String jsonErrorString = objectNode.toString();
		throw new WebApplicationException(
				Response.status(ApplicationConstants.RESPONSE_CODE_BAD_REQUEST).entity(jsonErrorString).type(MediaType.APPLICATION_JSON).build());
	}
}
