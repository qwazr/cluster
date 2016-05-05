/**
 * Copyright 2014-2016 Emmanuel Keller / QWAZR
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

package com.qwazr.cluster.manager;

import com.qwazr.utils.StringUtils;

import java.util.*;

final class ClusterNode {

	final Set<String> groups;
	final Set<String> services;

	volatile List<String> groupsCache;
	volatile List<String> servicesCache;

	ClusterNode() {
		this.groups = new HashSet<>();
		this.services = new HashSet<>();
		groupsCache = null;
		servicesCache = null;
	}

	final void registerGroup(final String group) {
		if (StringUtils.isEmpty(group))
			return;
		if (groups.contains(group))
			return;
		groups.add(group);
		groupsCache = new ArrayList<>(groups);
	}

	final void registerService(final String service) {
		if (StringUtils.isEmpty(service))
			return;
		if (services.contains(service))
			return;
		services.add(service);
		servicesCache = new ArrayList<>(services);
	}

	final Collection<String> getGroups() {
		return groupsCache;
	}

	final Collection<String> getServices() {
		return servicesCache;
	}

}
