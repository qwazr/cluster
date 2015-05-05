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
package com.qwazr.cluster.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/cluster")
public interface ClusterServiceInterface {

	public final String HEADER_CHECK_NAME = "X-OSS-CLUSTER-CHECK-TOKEN";

	@GET
	@Path("/")
	@Produces(MediaType.APPLICATION_JSON)
	public ClusterStatusJson list();

	@GET
	@Path("/nodes")
	@Produces(MediaType.APPLICATION_JSON)
	public Map<String, Set<String>> getNodes();

	@PUT
	@POST
	@Path("/")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public ClusterNodeStatusJson register(ClusterNodeRegisterJson register);

	@DELETE
	@Path("/")
	public Response unregister(@QueryParam("address") String address);

	@HEAD
	@Path("/")
	public Response check(@HeaderParam(HEADER_CHECK_NAME) String checkValue);

	@GET
	@Path("/services/{service_name}")
	@Produces(MediaType.APPLICATION_JSON)
	public ClusterServiceStatusJson getServiceStatus(
			@PathParam("service_name") String service_name);

	@GET
	@Path("/services/{service_name}/active")
	@Produces(MediaType.APPLICATION_JSON)
	public List<String> getActiveNodes(
			@PathParam("service_name") String service_name);

	@GET
	@Path("/services/{service_name}/active/random")
	@Produces(MediaType.TEXT_PLAIN)
	public String getActiveNodeRandom(
			@PathParam("service_name") String service_name);

}
