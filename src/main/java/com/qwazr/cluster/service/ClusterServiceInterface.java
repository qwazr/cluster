/**
 * Copyright 2015 Emmanuel Keller / QWAZR
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

import com.qwazr.cluster.ClusterServer;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;
import java.util.Set;

@RolesAllowed(ClusterServer.SERVICE_NAME_CLUSTER)
@Path("/cluster")
public interface ClusterServiceInterface {

	public final String HEADER_CHECK_NAME = "X-OSS-CLUSTER-CHECK-TOKEN";
	public final String HEADER_CHECK_ADDR = "X-OSS-CLUSTER-CHECK-ADDR";

	@GET
	@Path("/")
	@Produces(MediaType.APPLICATION_JSON)
	public ClusterStatusJson list();

	@GET
	@Path("/nodes")
	@Produces(MediaType.APPLICATION_JSON)
	public Map<String, Set<String>> getNodes();

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
	public Response check(@HeaderParam(HEADER_CHECK_NAME) String checkValue,
					@HeaderParam(HEADER_CHECK_ADDR) String checkAddr);

	@GET
	@Path("/services/{service_name}")
	@Produces(MediaType.APPLICATION_JSON)
	public ClusterServiceStatusJson getServiceStatus(@PathParam("service_name") String service_name);

	@GET
	@Path("/services/{service_name}/active")
	@Produces(MediaType.APPLICATION_JSON)
	public String[] getActiveNodes(@PathParam("service_name") String service_name);

	@GET
	@Path("/services/{service_name}/active/random")
	@Produces(MediaType.TEXT_PLAIN)
	public String getActiveNodeRandom(@PathParam("service_name") String service_name);

}
