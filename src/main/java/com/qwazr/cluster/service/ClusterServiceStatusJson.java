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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_NULL)
public class ClusterServiceStatusJson {

	public enum StatusEnum {
		ok, degraded, failure;
	}

	public final static List<String> EMPTY_LIST = new ArrayList<String>(0);
	public final static Map<String, ClusterNodeStatusJson> EMPTY_MAP = new HashMap<String, ClusterNodeStatusJson>();

	public final StatusEnum status;
	public final int active_count;
	public final List<String> active;
	public final int inactive_count;
	public final Map<String, ClusterNodeStatusJson> inactive;

	public ClusterServiceStatusJson() {
		this(EMPTY_LIST, EMPTY_MAP);
	}

	public ClusterServiceStatusJson(List<String> active,
			Map<String, ClusterNodeStatusJson> inactive) {
		this.active = active;
		this.inactive = inactive;
		this.active_count = active == null || active.isEmpty() ? 0 : active
				.size();
		this.inactive_count = inactive == null || inactive.isEmpty() ? 0
				: inactive.size();
		status = findStatus(active_count, inactive_count);
	}

	public static StatusEnum findStatus(int active_count, int inactive_count) {
		if (active_count == 0)
			return StatusEnum.failure;
		if (inactive_count == 0)
			return StatusEnum.ok;
		return StatusEnum.degraded;
	}
}
