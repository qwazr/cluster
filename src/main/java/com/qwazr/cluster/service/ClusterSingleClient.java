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

import com.fasterxml.jackson.core.type.TypeReference;
import com.qwazr.utils.UBuilder;
import com.qwazr.utils.http.HttpRequest;
import com.qwazr.utils.json.client.JsonClientAbstract;
import com.qwazr.utils.server.RemoteService;

import java.util.TreeMap;
import java.util.TreeSet;

class ClusterSingleClient extends JsonClientAbstract implements ClusterServiceInterface {

	ClusterSingleClient(RemoteService remote) {
		super(remote);
	}

	@Override
	public ClusterStatusJson list() {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/cluster");
		final HttpRequest request = HttpRequest.Get(uriBuilder.buildNoEx());
		return executeJson(request, null, null, ClusterStatusJson.class, valid200);
	}

	public final static TypeReference<TreeSet<String>> TreeSetStringClusterNodeJsonTypeRef =
			new TypeReference<TreeSet<String>>() {
			};

	@Override
	public TreeSet<String> getNodes() {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/cluster/nodes");
		final HttpRequest request = HttpRequest.Get(uriBuilder.buildNoEx());
		return executeJson(request, null, null, TreeSetStringClusterNodeJsonTypeRef, valid200);
	}

	public final static TypeReference<TreeMap<String, ClusterServiceStatusJson.StatusEnum>> MapStringStatusEnumTypeRef =
			new TypeReference<TreeMap<String, ClusterServiceStatusJson.StatusEnum>>() {
			};

	@Override
	public TreeMap<String, ClusterServiceStatusJson.StatusEnum> getServiceMap(String group) {
		final UBuilder uriBuilder =
				RemoteService.getNewUBuilder(remote, "/cluster/services").setParameter("group", group);
		final HttpRequest request = HttpRequest.Get(uriBuilder.buildNoEx());
		return executeJson(request, null, null, MapStringStatusEnumTypeRef, valid200);
	}

	@Override
	public ClusterServiceStatusJson getServiceStatus(final String service_name, final String group) {
		final UBuilder uriBuilder =
				RemoteService.getNewUBuilder(remote, "/cluster/services/", service_name).setParameter("group", group);
		final HttpRequest request = HttpRequest.Get(uriBuilder.buildNoEx());
		return executeJson(request, null, null, ClusterServiceStatusJson.class, valid200);
	}

	@Override
	public TreeSet<String> getActiveNodesByService(final String service_name, final String group) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/cluster/services/", service_name, "/active")
				.setParameter("group", group);
		final HttpRequest request = HttpRequest.Get(uriBuilder.buildNoEx());
		return executeJson(request, null, null, TreeSetStringClusterNodeJsonTypeRef, valid200);
	}

	@Override
	public String getActiveNodeRandomByService(final String service_name, final String group) {
		final UBuilder uriBuilder =
				RemoteService.getNewUBuilder(remote, "/cluster/services/" + service_name + "/active/random")
						.setParameter("group", group);
		final HttpRequest request = HttpRequest.Get(uriBuilder.buildNoEx());
		return executeString(request, null, null, valid200TextPlain);
	}

	@Override
	public String getActiveNodeLeaderByService(final String service_name, final String group) {
		final UBuilder uriBuilder =
				RemoteService.getNewUBuilder(remote, "/cluster/services/" + service_name + "/active/leader")
						.setParameter("group", group);
		final HttpRequest request = HttpRequest.Get(uriBuilder.buildNoEx());
		return executeString(request, null, null, valid200TextPlain);
	}

}
