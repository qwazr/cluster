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
import com.qwazr.utils.server.RemoteService;
import com.qwazr.utils.server.WebAppExceptionHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;

public class ClusterMultiClient extends JsonMultiClientAbstract<ClusterSingleClient>
		implements ClusterServiceInterface {

	private static final Logger logger = LoggerFactory.getLogger(ClusterMultiClient.class);

	public ClusterMultiClient(final ExecutorService executor, final RemoteService... remotes) {
		super(executor, new ClusterSingleClient[remotes.length], remotes);
	}

	@Override
	protected ClusterSingleClient newClient(RemoteService remote) {
		return new ClusterSingleClient(remote);
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
	public TreeMap<String, ClusterServiceStatusJson.StatusEnum> getServiceMap(String group) {
		WebAppExceptionHolder exceptionHolder = new WebAppExceptionHolder(logger);
		for (ClusterSingleClient client : this) {
			try {
				return client.getServiceMap(group);
			} catch (WebApplicationException e) {
				exceptionHolder.switchAndWarn(e);
			}
		}
		throw exceptionHolder.getException();
	}

	@Override
	public ClusterServiceStatusJson getServiceStatus(String service_name, String group) {
		WebAppExceptionHolder exceptionHolder = new WebAppExceptionHolder(logger);
		for (ClusterSingleClient client : this) {
			try {
				return client.getServiceStatus(service_name, group);
			} catch (WebApplicationException e) {
				exceptionHolder.switchAndWarn(e);
			}
		}
		throw exceptionHolder.getException();
	}

	@Override
	public String[] getActiveNodesByService(String service_name, String group_name) {
		WebAppExceptionHolder exceptionHolder = new WebAppExceptionHolder(logger);
		for (ClusterSingleClient client : this) {
			try {
				return client.getActiveNodesByService(service_name, group_name);
			} catch (WebApplicationException e) {
				exceptionHolder.switchAndWarn(e);
			}
		}
		throw exceptionHolder.getException();
	}

	@Override
	public String getActiveNodeRandomByService(String service_name, String group_name) {
		WebAppExceptionHolder exceptionHolder = new WebAppExceptionHolder(logger);
		for (ClusterSingleClient client : this) {
			try {
				return client.getActiveNodeRandomByService(service_name, group_name);
			} catch (WebApplicationException e) {
				exceptionHolder.switchAndWarn(e);
			}
		}
		throw exceptionHolder.getException();
	}

	@Override
	public String getActiveNodeMasterByService(String service_name, String group_name) {
		WebAppExceptionHolder exceptionHolder = new WebAppExceptionHolder(logger);
		for (ClusterSingleClient client : this) {
			try {
				return client.getActiveNodeMasterByService(service_name, group_name);
			} catch (WebApplicationException e) {
				exceptionHolder.switchAndWarn(e);
			}
		}
		throw exceptionHolder.getException();
	}

}
