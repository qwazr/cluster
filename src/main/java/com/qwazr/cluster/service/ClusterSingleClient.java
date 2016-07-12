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
import com.qwazr.utils.CharsetUtils;
import com.qwazr.utils.UBuilder;
import com.qwazr.utils.http.HttpUtils;
import com.qwazr.utils.json.client.JsonClientAbstract;
import com.qwazr.utils.server.RemoteService;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.util.TreeMap;
import java.util.TreeSet;

public class ClusterSingleClient extends JsonClientAbstract implements ClusterServiceInterface {

	public ClusterSingleClient(RemoteService remote) {
		super(remote);
	}

	@Override
	public ClusterStatusJson list() {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/cluster");
		Request request = Request.Get(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, ClusterStatusJson.class, 200);
	}

	public final static TypeReference<TreeSet<String>> TreeSetStringClusterNodeJsonTypeRef =
			new TypeReference<TreeSet<String>>() {
			};

	@Override
	public TreeSet<String> getNodes() {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/cluster/nodes");
		Request request = Request.Get(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, TreeSetStringClusterNodeJsonTypeRef, 200);
	}

	public final static TypeReference<TreeMap<String, ClusterServiceStatusJson.StatusEnum>> MapStringStatusEnumTypeRef =
			new TypeReference<TreeMap<String, ClusterServiceStatusJson.StatusEnum>>() {
			};

	@Override
	public TreeMap<String, ClusterServiceStatusJson.StatusEnum> getServiceMap(String group) {
		final UBuilder uriBuilder =
				RemoteService.getNewUBuilder(remote, "/cluster/services").setParameter("group", group);
		Request request = Request.Get(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, MapStringStatusEnumTypeRef, 200);
	}

	@Override
	public ClusterServiceStatusJson getServiceStatus(String service_name, String group) {
		final UBuilder uriBuilder =
				RemoteService.getNewUBuilder(remote, "/cluster/services/", service_name).setParameter("group", group);
		Request request = Request.Get(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, ClusterServiceStatusJson.class, 200);
	}

	@Override
	public TreeSet<String> getActiveNodesByService(String service_name, String group) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/cluster/services/", service_name, "/active")
				.setParameter("group", group);
		Request request = Request.Get(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, TreeSetStringClusterNodeJsonTypeRef, 200);
	}

	@Override
	public String getActiveNodeRandomByService(String service_name, String group) {
		try {
			final UBuilder uriBuilder =
					RemoteService.getNewUBuilder(remote, "/cluster/services/" + service_name + "/active/random")
							.setParameter("group", group);
			Request request = Request.Get(uriBuilder.buildNoEx());
			HttpResponse response = execute(request, null, null);
			HttpUtils.checkStatusCodes(response, 200);
			return IOUtils.toString(HttpUtils.checkIsEntity(response, ContentType.TEXT_PLAIN).getContent(),
					CharsetUtils.CharsetUTF8);
		} catch (IOException e) {
			throw new WebApplicationException(e.getMessage(), e, Status.INTERNAL_SERVER_ERROR);
		}
	}

	@Override
	public String getActiveNodeLeaderByService(String service_name, String group) {
		try {
			final UBuilder uriBuilder =
					RemoteService.getNewUBuilder(remote, "/cluster/services/" + service_name + "/active/leader")
							.setParameter("group", group);
			Request request = Request.Get(uriBuilder.buildNoEx());
			HttpResponse response = execute(request, null, null);
			HttpUtils.checkStatusCodes(response, 200);
			return IOUtils.toString(HttpUtils.checkIsEntity(response, ContentType.TEXT_PLAIN).getContent(),
					CharsetUtils.CharsetUTF8);
		} catch (IOException e) {
			throw new WebApplicationException(e.getMessage(), e, Status.INTERNAL_SERVER_ERROR);
		}
	}

}
