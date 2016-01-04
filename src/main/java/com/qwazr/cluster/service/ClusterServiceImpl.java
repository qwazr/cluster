/**
 * Copyright 2015-2016 Emmanuel Keller / QWAZR
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qwazr.cluster.service;

import com.qwazr.cluster.manager.ClusterManager;
import com.qwazr.cluster.manager.ClusterNode;
import com.qwazr.utils.server.ServerException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClusterServiceImpl implements ClusterServiceInterface {

	@Context
	HttpServletRequest request;

	@Override
	public ClusterStatusJson list() {
		try {
			//System.out.println("PRINCIPAL: " + request.getUserPrincipal().getName());
			//if (!request.isUserInRole(ClusterServer.SERVICE_NAME_CLUSTER))
			//	throw new ServerException(Status.UNAUTHORIZED);
			ClusterManager manager = ClusterManager.INSTANCE;
			List<ClusterNode> clusterNodeList = manager.getNodeList();
			if (clusterNodeList == null)
				return null;
			ClusterStatusJson clusterStatus = new ClusterStatusJson(manager);
			for (ClusterNode clusterNode : clusterNodeList)
				clusterStatus.addNodeStatus(clusterNode);
			return clusterStatus;
		} catch (ServerException e) {
			throw e.getJsonException();
		}
	}

	@Override
	public Map<String, Set<String>> getNodes() {
		try {
			ClusterManager manager = ClusterManager.INSTANCE;
			Map<String, Set<String>> nodeMap = new HashMap<String, Set<String>>();
			List<ClusterNode> clusterNodeList;
			clusterNodeList = manager.getNodeList();
			if (clusterNodeList == null)
				return nodeMap;
			for (ClusterNode clusterNode : clusterNodeList)
				if (clusterNode.services != null && !clusterNode.services.isEmpty())
					nodeMap.put(clusterNode.address, clusterNode.services);
			return nodeMap;
		} catch (ServerException e) {
			throw e.getJsonException();
		}
	}

	@Override
	public Response check(String checkValue, String checkAddr) {
		ClusterManager.INSTANCE.check(checkAddr);
		return Response.ok().header(ClusterServiceInterface.HEADER_CHECK_NAME, checkValue).build();
	}

	@Override
	public ClusterNodeStatusJson register(ClusterNodeRegisterJson register) {
		if (register == null)
			throw new ServerException(Status.NOT_ACCEPTABLE).getJsonException();
		ClusterManager manager = ClusterManager.INSTANCE;
		try {
			ClusterNode clusterNode = manager.upsertNode(register.address, register.services);
			return clusterNode.getStatus();
		} catch (Exception e) {
			throw ServerException.getJsonException(e);
		}
	}

	@Override
	public Response unregister(String address) {
		if (address == null)
			throw new ServerException(Status.NOT_ACCEPTABLE).getJsonException();
		ClusterManager manager = ClusterManager.INSTANCE;
		try {
			ClusterNode clusterNode = manager.removeNode(address);
			return clusterNode == null ? Response.status(Status.NOT_FOUND).build() : Response.ok().build();
		} catch (Exception e) {
			throw ServerException.getTextException(e);
		}
	}

	@Override
	public String[] getActiveNodes(String service_name) {
		if (service_name == null)
			throw new ServerException(Status.NOT_ACCEPTABLE).getJsonException();
		ClusterManager manager = ClusterManager.INSTANCE;
		try {
			return manager.getActiveNodes(service_name);
		} catch (ServerException e) {
			throw e.getJsonException();
		}
	}

	@Override
	public String getActiveNodeRandom(String service_name) {
		if (service_name == null)
			throw new ServerException(Status.NOT_ACCEPTABLE).getJsonException();
		ClusterManager manager = ClusterManager.INSTANCE;
		try {
			return manager.getActiveNodeRandom(service_name);
		} catch (ServerException e) {
			throw e.getJsonException();
		}
	}

	@Override
	public ClusterServiceStatusJson getServiceStatus(String service_name) {
		ClusterManager manager = ClusterManager.INSTANCE;
		try {
			return manager.getServiceStatus(service_name);
		} catch (ServerException e) {
			throw e.getJsonException();
		}
	}

}
