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
package com.qwazr.cluster;

import com.qwazr.cluster.manager.ClusterManager;
import com.qwazr.utils.StringUtils;
import com.qwazr.utils.server.GenericServer;
import com.qwazr.utils.server.ServerBuilder;
import com.qwazr.utils.server.ServerConfiguration;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class ClusterServer {

	public static GenericServer start(final Collection<String> masters, final Collection<String> groups)
			throws Exception {
		final ServerBuilder builder = new ServerBuilder();
		ClusterManager.load(builder, masters, groups);
		return builder.build().start(true);
	}

	private static List<String> getList(final String key) {
		String values = System.getenv().get(key);
		if (values == null)
			values = System.getProperty(key);
		if (values == null)
			return null;
		return Arrays.asList(StringUtils.split(values, ", "));
	}

	public static void main(String[] args) throws Exception {
		start(getList("QWAZR_MASTERS"), getList("QWAZR_GROUPS"));
	}

}