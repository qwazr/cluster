/*
 * Copyright 2015-2017 Emmanuel Keller / QWAZR
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
package com.qwazr.cluster;

import com.qwazr.server.RemoteService;
import com.qwazr.server.client.JsonClient;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import java.util.TreeMap;
import java.util.TreeSet;

public class ClusterSingleClient extends JsonClient implements ClusterServiceInterface {

	final String serverAddress;
	final WebTarget clusterTarget;
	final WebTarget nodesTarget;
	final WebTarget servicesTarget;

	ClusterSingleClient(RemoteService remote) {
		super(remote);
		this.serverAddress = remote.serverAddress;
		final WebTarget rootTarget = client.target(remote.serviceAddress);
		clusterTarget = rootTarget.path("cluster");
		nodesTarget = clusterTarget.path("nodes");
		servicesTarget = clusterTarget.path("services");
	}

	@Override
	public boolean equals(Object o) {
		if (o == null)
			return false;
		if (o == this)
			return true;
		if (o instanceof ClusterServiceImpl)
			return serverAddress.equals(((ClusterServiceImpl) o).manager.me.httpAddressKey);
		if (o instanceof ClusterSingleClient)
			return serverAddress.equals(((ClusterSingleClient) o).serverAddress);
		return false;
	}

	@Override
	public ClusterStatusJson getStatus() {
		return clusterTarget.request().get(ClusterStatusJson.class);
	}

	private final static GenericType<TreeSet<String>> treeSetStringType = new GenericType<TreeSet<String>>() {
	};

	@Override
	public TreeSet<String> getNodes() {
		return nodesTarget.request(MediaType.APPLICATION_JSON).get(treeSetStringType);
	}

	private final static GenericType<TreeMap<String, ClusterServiceStatusJson.StatusEnum>> mapStringStatusEnumType =
			new GenericType<TreeMap<String, ClusterServiceStatusJson.StatusEnum>>() {
			};

	@Override
	public TreeMap<String, ClusterServiceStatusJson.StatusEnum> getServiceMap(String group) {
		return (group == null ? servicesTarget : servicesTarget.queryParam("group", group)).request(
				MediaType.APPLICATION_JSON).get(mapStringStatusEnumType);
	}

	@Override
	public ClusterServiceStatusJson getServiceStatus(final String serviceName, final String group) {
		final WebTarget target = servicesTarget.path(serviceName);
		return (group == null ? target : target.queryParam("group", group)).request(MediaType.APPLICATION_JSON)
				.get(ClusterServiceStatusJson.class);
	}

	@Override
	public TreeSet<String> getActiveNodesByService(final String serviceName, final String group) {
		final WebTarget target = servicesTarget.path(serviceName).path("active");
		return (group == null ? target : target.queryParam("group", group)).request(MediaType.APPLICATION_JSON)
				.get(treeSetStringType);
	}

	@Override
	public String getActiveNodeRandomByService(final String serviceName, final String group) {
		final WebTarget target = servicesTarget.path(serviceName).path("active").path("random");
		return (group == null ? target : target.queryParam("group", group)).request(MediaType.TEXT_PLAIN)
				.get(String.class);
	}

	@Override
	public String getActiveNodeLeaderByService(final String serviceName, final String group) {
		final WebTarget target = servicesTarget.path(serviceName).path("active").path("leader");
		return (group == null ? target : target.queryParam("group", group)).request(MediaType.TEXT_PLAIN)
				.get(String.class);
	}

}
