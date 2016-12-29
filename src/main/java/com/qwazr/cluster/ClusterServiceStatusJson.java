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
package com.qwazr.cluster;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.qwazr.utils.StringUtils;

import java.util.SortedSet;

@JsonInclude(Include.NON_NULL)
public class ClusterServiceStatusJson {

	public enum StatusEnum {
		ok, failure
	}

	public final String leader;
	public final StatusEnum status;
	public final int active_count;
	public final SortedSet<String> active;

	public ClusterServiceStatusJson() {
		leader = StringUtils.EMPTY;
		status = null;
		active_count = 0;
		active = null;
	}

	public ClusterServiceStatusJson(final SortedSet<String> nodes) {
		this.active = nodes;
		this.leader = nodes != null ? !nodes.isEmpty() ? nodes.first() : null : null;
		this.active_count = active == null ? 0 : active.size();
		status = findStatus(active_count);
	}

	public static StatusEnum findStatus(int active_count) {
		if (active_count == 0)
			return StatusEnum.failure;
		return StatusEnum.ok;
	}
}
