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

import com.fasterxml.jackson.core.type.TypeReference;
import com.qwazr.cluster.service.*;
import com.qwazr.utils.http.HttpUtils;
import com.qwazr.utils.json.client.JsonClientAbstract;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;

public class ClusterSingleClient extends JsonClientAbstract implements ClusterServiceInterface {

	public ClusterSingleClient(String url, int msTimeOut) throws URISyntaxException {
		super(url, msTimeOut);
	}

	@Override
	public ClusterStatusJson list() {
		UBuilder uriBuilder = new UBuilder("/cluster");
		Request request = Request.Get(uriBuilder.build());
		return commonServiceRequest(request, null, msTimeOut, ClusterStatusJson.class, 200);
	}

	public final static TypeReference<Map<String, Set<String>>> MapStringSetStringTypeRef = new TypeReference<Map<String, Set<String>>>() {
	};

	@Override
	public Map<String, Set<String>> getNodes() {
		UBuilder uriBuilder = new UBuilder("/cluster/nodes");
		Request request = Request.Get(uriBuilder.build());
		return commonServiceRequest(request, null, msTimeOut, MapStringSetStringTypeRef,
						200);
	}

	@Override
	public ClusterNodeStatusJson register(ClusterNodeRegisterJson register) {
		UBuilder uriBuilder = new UBuilder("/cluster");
		Request request = Request.Post(uriBuilder.build());
		return commonServiceRequest(request, register, msTimeOut, ClusterNodeStatusJson.class, 200);
	}

	@Override
	public Response unregister(String address) {
		try {
			UBuilder uriBuilder = new UBuilder("/cluster");
			uriBuilder.setParameter("address", address);
			Request request = Request.Delete(uriBuilder.build());
			HttpResponse response = execute(request, null, msTimeOut);
			HttpUtils.checkStatusCodes(response, 200);
			return Response.status(response.getStatusLine().getStatusCode()).build();
		} catch (IOException e) {
			throw new WebApplicationException(e.getMessage(), e, Status.INTERNAL_SERVER_ERROR);
		}
	}

	@Override
	public Response check(String checkValue, String checkAddr) {
		return Response.status(Status.NOT_IMPLEMENTED).build();
	}

	@Override
	public ClusterServiceStatusJson getServiceStatus(String service_name) {
		UBuilder uriBuilder = new UBuilder("/cluster/services/", service_name);
		Request request = Request.Get(uriBuilder.build());
		return commonServiceRequest(request, null, msTimeOut, ClusterServiceStatusJson.class, 200);
	}

	@Override
	public String[] getActiveNodes(String service_name) {
		UBuilder uriBuilder = new UBuilder("/cluster/services/", service_name, "/active");
		Request request = Request.Get(uriBuilder.build());
		return commonServiceRequest(request, null, msTimeOut, String[].class, 200);
	}

	@Override
	public String getActiveNodeRandom(String service_name) {
		try {
			UBuilder uriBuilder = new UBuilder("/cluster/services/", service_name, "/active/random");
			Request request = Request.Get(uriBuilder.build());
			HttpResponse response = execute(request, null, msTimeOut);
			HttpUtils.checkStatusCodes(response, 200);
			return IOUtils.toString(HttpUtils.checkIsEntity(response, ContentType.TEXT_PLAIN).getContent());
		} catch (IOException e) {
			throw new WebApplicationException(e.getMessage(), e, Status.INTERNAL_SERVER_ERROR);
		}
	}

}
