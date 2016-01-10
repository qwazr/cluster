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
import com.qwazr.cluster.manager.ClusterNode;

import java.util.Set;

@JsonInclude(Include.NON_NULL)
public class ClusterNodeJson {

	public final String address;
	public final Set<String> services;
	public final Set<String> groups;

	public ClusterNodeJson() {
		address = null;
		services = null;
		groups = null;
	}

	public ClusterNodeJson(String address, Set<String> services, Set<String> groups) {
		this.address = address;
		this.services = services;
		this.groups = groups;
	}

	public ClusterNodeJson(ClusterNode clusterNode) {
		this.address = clusterNode.address;
		this.services = clusterNode.services;
		this.groups = clusterNode.groups;
	}
}
