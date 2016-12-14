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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.qwazr.cluster.service.ClusterServiceStatusJson.StatusEnum;
import com.qwazr.server.ServerException;

import java.util.Date;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

@JsonInclude(Include.NON_NULL)
public class ClusterStatusJson {

	public final String me;
	public final String webapp;
	public final TreeMap<String, TreeSet<String>> groups;
	public final TreeMap<String, StatusEnum> services;
	public final Date last_keep_alive_execution;
	public final TreeMap<String, ClusterNodeJson> active_nodes;
	public final TreeSet<String> masters;

	public ClusterStatusJson() {
		me = null;
		webapp = null;
		groups = null;
		services = null;
		last_keep_alive_execution = null;
		active_nodes = null;
		masters = null;
	}

	public ClusterStatusJson(final String me, final String webapp, final TreeMap<String, ClusterNodeJson> nodesMap,
			final TreeMap<String, TreeSet<String>> groups, final TreeMap<String, TreeSet<String>> services,
			final Set<String> masters, final Date lastKeepAliveExecution) throws ServerException {
		this.me = me;
		this.webapp = webapp;
		this.groups = groups;
		this.services = new TreeMap<>();
		if (services != null) {
			services.forEach((service, nodesSet) -> this.services.put(service,
					ClusterServiceStatusJson.findStatus(nodesSet.size())));
		}
		this.masters = masters == null || masters.isEmpty() ? null : new TreeSet<>(masters);
		this.last_keep_alive_execution = lastKeepAliveExecution;
		this.active_nodes = nodesMap;
	}

}
