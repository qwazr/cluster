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
import com.qwazr.utils.StringUtils;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Collections;
import java.util.Map;

@JsonInclude(Include.NON_NULL)
public class ClusterServiceStatusJson {

	public enum StatusEnum {
		ok, degraded, failure
	}

	public final String leader;
	public final StatusEnum status;
	public final int active_count;
	public final String[] active;
	public final int inactive_count;
	public final Map<String, ClusterNodeStatusJson> inactive;

	public ClusterServiceStatusJson() {
		this(StringUtils.EMPTY, ArrayUtils.EMPTY_STRING_ARRAY, Collections.emptyMap());
	}

	public ClusterServiceStatusJson(String leader, String[] active, Map<String, ClusterNodeStatusJson> inactive) {
		this.active = active;
		this.inactive = inactive;
		this.leader = leader;
		this.active_count = active == null ? 0 : active.length;
		this.inactive_count = inactive == null || inactive.isEmpty() ? 0 : inactive.size();
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
