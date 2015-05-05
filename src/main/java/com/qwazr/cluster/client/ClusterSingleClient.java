/**
 * Copyright 2015 OpenSearchServer Inc.
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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;

import com.fasterxml.jackson.core.type.TypeReference;
import com.qwazr.cluster.service.ClusterNodeRegisterJson;
import com.qwazr.cluster.service.ClusterNodeStatusJson;
import com.qwazr.cluster.service.ClusterServiceInterface;
import com.qwazr.cluster.service.ClusterServiceStatusJson;
import com.qwazr.cluster.service.ClusterStatusJson;
import com.qwazr.utils.http.HttpUtils;
import com.qwazr.utils.json.client.JsonClientAbstract;

public class ClusterSingleClient extends JsonClientAbstract implements
		ClusterServiceInterface {

	public ClusterSingleClient(String url, int msTimeOut)
			throws URISyntaxException {
		super(url, msTimeOut);
	}

	@Override
	public ClusterStatusJson list() {
		try {
			URIBuilder uriBuilder = getBaseUrl("/cluster");
			Request request = Request.Get(uriBuilder.build());
			return execute(request, null, msTimeOut, ClusterStatusJson.class,
					200);
		} catch (URISyntaxException | IOException e) {
			throw new WebApplicationException(e.getMessage(), e,
					Status.INTERNAL_SERVER_ERROR);
		}
	}

	public final static TypeReference<Map<String, Set<String>>> MapStringSetStringTypeRef = new TypeReference<Map<String, Set<String>>>() {
	};

	@Override
	public Map<String, Set<String>> getNodes() {
		try {
			URIBuilder uriBuilder = getBaseUrl("/cluster/nodes");
			Request request = Request.Get(uriBuilder.build());
			return (Map<String, Set<String>>) execute(request, null, msTimeOut,
					MapStringSetStringTypeRef, 200);
		} catch (URISyntaxException | IOException e) {
			throw new WebApplicationException(e.getMessage(), e,
					Status.INTERNAL_SERVER_ERROR);
		}
	}

	@Override
	public ClusterNodeStatusJson register(ClusterNodeRegisterJson register) {
		try {
			URIBuilder uriBuilder = getBaseUrl("/cluster");
			Request request = Request.Post(uriBuilder.build());
			return execute(request, register, msTimeOut,
					ClusterNodeStatusJson.class, 200);
		} catch (URISyntaxException | IOException e) {
			throw new WebApplicationException(e.getMessage(), e,
					Status.INTERNAL_SERVER_ERROR);
		}
	}

	@Override
	public Response unregister(String address) {
		try {
			URIBuilder uriBuilder = getBaseUrl("/cluster");
			uriBuilder.setParameter("address", address);
			Request request = Request.Delete(uriBuilder.build());
			HttpResponse response = execute(request, null, msTimeOut);
			HttpUtils.checkStatusCodes(response, 200);
			return Response.status(response.getStatusLine().getStatusCode())
					.build();
		} catch (URISyntaxException | IOException e) {
			throw new WebApplicationException(e.getMessage(), e,
					Status.INTERNAL_SERVER_ERROR);
		}
	}

	@Override
	public Response check(String checkValue) {
		return Response.status(Status.NOT_IMPLEMENTED).build();
	}

	@Override
	public ClusterServiceStatusJson getServiceStatus(String service_name) {
		try {
			URIBuilder uriBuilder = getBaseUrl("/cluster/services/",
					service_name);
			Request request = Request.Get(uriBuilder.build());
			return execute(request, null, msTimeOut,
					ClusterServiceStatusJson.class, 200);
		} catch (URISyntaxException | IOException e) {
			throw new WebApplicationException(e.getMessage(), e,
					Status.INTERNAL_SERVER_ERROR);
		}
	}

	public final static TypeReference<List<String>> ListStringTypeRef = new TypeReference<List<String>>() {
	};

	@Override
	public List<String> getActiveNodes(String service_name) {
		try {
			URIBuilder uriBuilder = getBaseUrl("/cluster/services/",
					service_name, "/active");
			Request request = Request.Get(uriBuilder.build());
			return (List<String>) execute(request, null, msTimeOut,
					ListStringTypeRef, 200);
		} catch (URISyntaxException | IOException e) {
			throw new WebApplicationException(e.getMessage(), e,
					Status.INTERNAL_SERVER_ERROR);
		}
	}

	@Override
	public String getActiveNodeRandom(String service_name) {
		try {
			URIBuilder uriBuilder = getBaseUrl("/cluster/services/",
					service_name, "/active/random");
			Request request = Request.Get(uriBuilder.build());
			HttpResponse response = execute(request, null, msTimeOut);
			HttpUtils.checkStatusCodes(response, 200);
			return IOUtils.toString(HttpUtils.checkIsEntity(response,
					ContentType.TEXT_PLAIN).getContent());
		} catch (URISyntaxException | IOException e) {
			throw new WebApplicationException(e.getMessage(), e,
					Status.INTERNAL_SERVER_ERROR);
		}
	}

}
