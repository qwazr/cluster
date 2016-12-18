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
package com.qwazr.cluster.service;

import com.qwazr.cluster.manager.ClusterManager;
import com.qwazr.server.AbstractServiceImpl;
import com.qwazr.server.ServerException;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response.Status;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;

public class ClusterServiceImpl extends AbstractServiceImpl implements ClusterServiceInterface {

	@Context
	HttpServletRequest request;

	private ClusterManager manager;

	public ClusterServiceImpl(final ClusterManager manager) {
		this.manager = manager;
	}

	public ClusterServiceImpl() {
		this(null);
	}

	@PostConstruct
	public void init() {
		this.manager = ClusterManager.getInstance(context);
	}

	@Override
	public ClusterStatusJson getStatus() {
		try {
			return manager.getStatus();
		} catch (ServerException e) {
			throw e.getJsonException();
		}
	}

	@Override
	public SortedSet<String> getNodes() {
		try {
			return new TreeSet<>(manager.getNodes());
		} catch (ServerException e) {
			throw e.getJsonException();
		}
	}

	@Override
	public SortedSet<String> getActiveNodesByService(String service_name, String group) {
		if (service_name == null)
			throw new ServerException(Status.NOT_ACCEPTABLE).getJsonException();
		try {
			return manager.getNodesByGroupByService(group, service_name);
		} catch (ServerException e) {
			throw e.getJsonException();
		}
	}

	@Override
	public String getActiveNodeRandomByService(String service_name, String group) {
		if (service_name == null)
			throw new ServerException(Status.NOT_ACCEPTABLE).getJsonException();
		try {
			return manager.getRandomNode(group, service_name);
		} catch (ServerException e) {
			throw e.getJsonException();
		}
	}

	@Override
	public String getActiveNodeLeaderByService(String service_name, String group) {
		if (service_name == null)
			throw new ServerException(Status.NOT_ACCEPTABLE).getJsonException();
		try {
			return manager.getLeaderNode(group, service_name);
		} catch (ServerException e) {
			throw e.getJsonException();
		}
	}

	@Override
	public SortedMap<String, ClusterServiceStatusJson.StatusEnum> getServiceMap(String group) {
		try {
			return manager.getServicesStatus(group);
		} catch (ServerException e) {
			throw e.getJsonException();
		}
	}

	@Override
	public ClusterServiceStatusJson getServiceStatus(String service_name, String group) {
		try {
			return manager.getServiceStatus(group, service_name);
		} catch (ServerException e) {
			throw e.getJsonException();
		}
	}

}
