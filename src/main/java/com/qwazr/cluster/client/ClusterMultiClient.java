/**
 * Copyright 2015 Emmanuel Keller / QWAZR
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qwazr.cluster.client;

import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qwazr.cluster.service.ClusterNodeRegisterJson;
import com.qwazr.cluster.service.ClusterNodeStatusJson;
import com.qwazr.cluster.service.ClusterServiceInterface;
import com.qwazr.cluster.service.ClusterServiceStatusJson;
import com.qwazr.cluster.service.ClusterStatusJson;
import com.qwazr.utils.json.client.JsonMultiClientAbstract;

public class ClusterMultiClient extends
		JsonMultiClientAbstract<String, ClusterSingleClient> implements
		ClusterServiceInterface {

	private static final Logger logger = LoggerFactory
			.getLogger(ClusterMultiClient.class);

	public ClusterMultiClient(String[] urls, int msTimeOut)
			throws URISyntaxException {
		// TODO Pass executor
		super(null, new ClusterSingleClient[urls.length], urls, msTimeOut);
	}

	@Override
	protected ClusterSingleClient newClient(String url, int msTimeOut)
			throws URISyntaxException {
		return new ClusterSingleClient(url, msTimeOut);
	}

	@Override
	public ClusterStatusJson list() {
		WebAppExceptionHolder exceptionHolder = new WebAppExceptionHolder(
				logger);
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
	public Map<String, Set<String>> getNodes() {
		WebAppExceptionHolder exceptionHolder = new WebAppExceptionHolder(
				logger);
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
	public ClusterNodeStatusJson register(ClusterNodeRegisterJson register) {
		ClusterNodeStatusJson result = null;
		for (ClusterSingleClient client : this) {
			try {
				result = client.register(register);
			} catch (WebApplicationException e) {
				logger.warn(e.getMessage(), e);
			}
		}
		return result;
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
	public Response check(String checkValue) {
		return Response.status(Status.NOT_IMPLEMENTED).build();
	}

	@Override
	public ClusterServiceStatusJson getServiceStatus(String service_name) {
		WebAppExceptionHolder exceptionHolder = new WebAppExceptionHolder(
				logger);
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
	public String[] getActiveNodes(String service_name) {
		WebAppExceptionHolder exceptionHolder = new WebAppExceptionHolder(
				logger);
		for (ClusterSingleClient client : this) {
			try {
				return client.getActiveNodes(service_name);
			} catch (WebApplicationException e) {
				exceptionHolder.switchAndWarn(e);
			}
		}
		throw exceptionHolder.getException();
	}

	@Override
	public String getActiveNodeRandom(String service_name) {
		WebAppExceptionHolder exceptionHolder = new WebAppExceptionHolder(
				logger);
		for (ClusterSingleClient client : this) {
			try {
				return client.getActiveNodeRandom(service_name);
			} catch (WebApplicationException e) {
				exceptionHolder.switchAndWarn(e);
			}
		}
		throw exceptionHolder.getException();
	}

}
