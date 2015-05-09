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

import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.qwazr.cluster.manager.ClusterManager;
import com.qwazr.cluster.manager.ClusterNode;
import com.qwazr.cluster.service.ClusterServiceStatusJson.StatusEnum;

@JsonInclude(Include.NON_NULL)
public class ClusterStatusJson {

	public final boolean is_master;
	public final Set<String> active_nodes;
	public final Map<String, ClusterNodeStatusJson> inactive_nodes;
	public final Map<String, StatusEnum> services;
	public final String[] masters;
	public final Map<String, Date> last_executions;

	public ClusterStatusJson() {
		is_master = false;
		active_nodes = null;
		inactive_nodes = null;
		services = null;
		masters = null;
		last_executions = null;
	}

	public ClusterStatusJson(ClusterManager clusterManager) {
		this.is_master = clusterManager.isMaster();
		this.active_nodes = new TreeSet<String>();
		this.inactive_nodes = new TreeMap<String, ClusterNodeStatusJson>();
		this.services = clusterManager.getServicesStatus();
		this.masters = clusterManager.getMasterArray();
		this.last_executions = clusterManager.getLastExecutions();
	}

	public void addNodeStatus(ClusterNode node) {
		if (node.isActive())
			active_nodes.add(node.address);
		else
			inactive_nodes.put(node.address, node.getStatus());
	}
}
