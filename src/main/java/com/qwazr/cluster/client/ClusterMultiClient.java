/**
 * Copyright 2015-2016 Emmanuel Keller / QWAZR
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qwazr.cluster.client;

import com.qwazr.cluster.service.*;
import com.qwazr.utils.json.client.JsonMultiClientAbstract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.net.URISyntaxException;
import java.util.Map;

public class ClusterMultiClient extends JsonMultiClientAbstract<String, ClusterSingleClient>
		implements ClusterServiceInterface {

	private static final Logger logger = LoggerFactory.getLogger(ClusterMultiClient.class);

	public ClusterMultiClient(String[] urls, Integer msTimeOut) throws URISyntaxException {
		// TODO Pass executor
		super(null, new ClusterSingleClient[urls.length], urls, msTimeOut);
	}

	@Override
	protected ClusterSingleClient newClient(String url, Integer msTimeOut) throws URISyntaxException {
		return new ClusterSingleClient(url, msTimeOut);
	}

	@Override
	public ClusterStatusJson list() {
		WebAppExceptionHolder exceptionHolder = new WebAppExceptionHolder(logger);
		for (ClusterSingleClient client : this) {
			try {
				return client.list();
			} catch (WebApplicationException e) {
				exceptionHolder.switchAndWarn(e);
			}
		}
		throw exceptionHolder.getException();
	}

	@Override
	public Map<String, ClusterNodeJson> getNodes() {
		WebAppExceptionHolder exceptionHolder = new WebAppExceptionHolder(logger);
		for (ClusterSingleClient client : this) {
			try {
				return client.getNodes();
			} catch (WebApplicationException e) {
				exceptionHolder.switchAndWarn(e);
			}
		}
		throw exceptionHolder.getException();
	}

	@Override
	public ClusterNodeStatusJson register(ClusterNodeJson register) {
		WebAppExceptionHolder exceptionHolder = new WebAppExceptionHolder(logger);
		ClusterNodeStatusJson result = null;
		for (ClusterSingleClient client : this) {
			try {
				result = client.register(register);
			} catch (WebApplicationException e) {
				exceptionHolder.switchAndWarn(e);
			}
		}
		if (result != null)
			return result;
		throw exceptionHolder.getException();
	}

	@Override
	public Response unregister(String address) {
		for (ClusterSingleClient client : this) {
			try {
				client.unregister(address);
			} catch (WebApplicationException e) {
				logger.warn(e.getMessage(), e);
			}
		}
		return Response.ok().build();
	}

	@Override
	public Response check(String checkValue, String checkAddr) {
		return Response.status(Status.NOT_IMPLEMENTED).build();
	}

	@Override
	public ClusterKeyStatusJson getServiceStatus(String service_name) {
		WebAppExceptionHolder exceptionHolder = new WebAppExceptionHolder(logger);
		for (ClusterSingleClient client : this) {
			try {
				return client.getServiceStatus(service_name);
			} catch (WebApplicationException e) {
				exceptionHolder.switchAndWarn(e);
			}
		}
		throw exceptionHolder.getException();
	}

	@Override
	public ClusterKeyStatusJson getGroupStatus(String group_name) {
		WebAppExceptionHolder exceptionHolder = new WebAppExceptionHolder(logger);
		for (ClusterSingleClient client : this) {
			try {
				return client.getGroupStatus(group_name);
			} catch (WebApplicationException e) {
				exceptionHolder.switchAndWarn(e);
			}
		}
		throw exceptionHolder.getException();
	}

	@Override
	public String[] getActiveNodesByService(String service_name) {
		WebAppExceptionHolder exceptionHolder = new WebAppExceptionHolder(logger);
		for (ClusterSingleClient client : this) {
			try {
				return client.getActiveNodesByService(service_name);
			} catch (WebApplicationException e) {
				exceptionHolder.switchAndWarn(e);
			}
		}
		throw exceptionHolder.getException();
	}

	@Override
	public String[] getActiveNodesByGroup(String group_name) {
		WebAppExceptionHolder exceptionHolder = new WebAppExceptionHolder(logger);
		for (ClusterSingleClient client : this) {
			try {
				return client.getActiveNodesByGroup(group_name);
			} catch (WebApplicationException e) {
				exceptionHolder.switchAndWarn(e);
			}
		}
		throw exceptionHolder.getException();
	}

	@Override
	public String getActiveNodeRandomByService(String service_name) {
		WebAppExceptionHolder exceptionHolder = new WebAppExceptionHolder(logger);
		for (ClusterSingleClient client : this) {
			try {
				return client.getActiveNodeRandomByService(service_name);
			} catch (WebApplicationException e) {
				exceptionHolder.switchAndWarn(e);
			}
		}
		throw exceptionHolder.getException();
	}

	@Override
	public String getActiveNodeRandomByGroup(String group_name) {
		WebAppExceptionHolder exceptionHolder = new WebAppExceptionHolder(logger);
		for (ClusterSingleClient client : this) {
			try {
				return client.getActiveNodeRandomByGroup(group_name);
			} catch (WebApplicationException e) {
				exceptionHolder.switchAndWarn(e);
			}
		}
		throw exceptionHolder.getException();
	}

}
