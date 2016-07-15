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
package com.qwazr.cluster.test;

import com.google.common.io.Files;
import com.qwazr.cluster.ClusterServer;
import com.qwazr.cluster.manager.ClusterManager;
import com.qwazr.cluster.service.ClusterServiceInterface;
import com.qwazr.cluster.service.ClusterSingleClient;
import com.qwazr.utils.StringUtils;
import com.qwazr.utils.process.ProcessUtils;
import com.qwazr.utils.server.RemoteService;
import com.qwazr.utils.server.UdpServerThread;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestServer {

	final File dataDir;
	final Process process;
	final ClusterServiceInterface client;
	final String address;

	TestServer(final List<String> masters, final int port, final List<String> groups) throws Exception {

		dataDir = Files.createTempDir();
		address = "http://localhost:" + port;

		Map<String, String> env = new HashMap<>();
		env.put("QWAZR_DATA", dataDir.getAbsolutePath());
		if (groups != null)
			env.put("QWAZR_GROUPS", StringUtils.join(groups, ","));
		if (masters != null)
			env.put("QWAZR_MASTERS", StringUtils.join(masters, ","));
		env.put("PUBLIC_ADDR", "localhost");
		env.put("LISTEN_ADDR", "localhost");
		env.put("WEBSERVICE_PORT", Integer.toString(port));
		env.put("UDP_ADDRESS", UdpServerThread.DEFAULT_MULTICAST);

		client = new ClusterSingleClient(new RemoteService(address));

		if (ClusterManager.INSTANCE != null) {
			process = ProcessUtils.java(ClusterServer.class, env);
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					if (process.isAlive())
						process.destroy();
				}
			});
		} else {
			env.forEach(System::setProperty);
			ClusterServer.start(masters, groups);
			process = null;
		}

	}

}
