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
import com.qwazr.utils.server.ServiceInterface;
import com.qwazr.utils.server.ServiceName;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;
import java.util.TreeMap;

@RolesAllowed(ClusterManager.SERVICE_NAME_CLUSTER)
@Path("/cluster")
@ServiceName("cluster")
public interface ClusterServiceInterface extends ServiceInterface {

	String HEADER_CHECK_NAME = "X-OSS-CLUSTER-CHECK-TOKEN";
	String HEADER_CHECK_ADDR = "X-OSS-CLUSTER-CHECK-ADDR";

	@GET
	@Path("/")
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	ClusterStatusJson list();

	@GET
	@Path("/nodes")
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	Map<String, ClusterNodeJson> getNodes();

	@POST
	@Path("/")
	@Consumes(ServiceInterface.APPLICATION_JSON_UTF8)
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	ClusterNodeStatusJson register(ClusterNodeJson register);

	@DELETE
	@Path("/")
	Response unregister(@QueryParam("address") String address);

	@HEAD
	@Path("/")
	Response check(@HeaderParam(HEADER_CHECK_NAME) String checkValue, @HeaderParam(HEADER_CHECK_ADDR) String checkAddr);

	@GET
	@Path("/services")
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	TreeMap<String, ClusterServiceStatusJson.StatusEnum> getServiceMap(@QueryParam("group") String group);

	@GET
	@Path("/services/{service_name}")
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	ClusterServiceStatusJson getServiceStatus(@PathParam("service_name") String service_name,
					@QueryParam("group") String group);

	@GET
	@Path("/services/{service_name}/active")
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	String[] getActiveNodesByService(@PathParam("service_name") String service_name, @QueryParam("group") String group);

	@GET
	@Path("/services/{service_name}/active/random")
	@Produces(MediaType.TEXT_PLAIN)
	String getActiveNodeRandomByService(@PathParam("service_name") String service_name,
					@QueryParam("group") String group);

	@GET
	@Path("/services/{service_name}/active/master")
	@Produces(MediaType.TEXT_PLAIN)
	String getActiveNodeMasterByService(@PathParam("service_name") String service_name,
					@QueryParam("group") String group);

}
