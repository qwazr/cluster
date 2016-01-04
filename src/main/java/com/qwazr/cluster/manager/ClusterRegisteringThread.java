/**
 * Copyright 2014-2016 Emmanuel Keller / QWAZR
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qwazr.cluster.manager;

import com.qwazr.cluster.client.ClusterMultiClient;
import com.qwazr.cluster.service.ClusterNodeRegisterJson;
import com.qwazr.utils.threads.PeriodicThread;
import com.qwazr.utils.threads.ThreadUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import java.util.Date;

public class ClusterRegisteringThread extends PeriodicThread {

	private static final Logger logger = LoggerFactory
			.getLogger(ClusterRegisteringThread.class);

	private final ClusterMultiClient clusterClient;

	private final String[] services;

	ClusterRegisteringThread(int monitoring_period_seconds, ClusterMultiClient clusterClient, String... services) {
		super("Nodes registration", monitoring_period_seconds);
		setDaemon(true);
		this.clusterClient = clusterClient;
		this.services = services;
		start();
	}

	@Override
	public void runner() {
		long removeTime = System.currentTimeMillis() - 150000;
		Long lastCheck = ClusterManager.INSTANCE.getLastCheck();
		if (lastCheck == null || lastCheck < removeTime) {
			try {
				if (logger.isInfoEnabled())
					logger.info("Registering to the masters: "
							+ StringUtils.join(services, ' ') + " last check: " +
							(lastCheck == null ? 0 : new Date(lastCheck)));
				clusterClient.register(new ClusterNodeRegisterJson(ClusterManager.INSTANCE.myAddress, services));
			} catch (WebApplicationException e) {
				logger.error("Registration failed", e);
			}
		}
		ClusterManager.INSTANCE.removeOldCheck(removeTime);
	}

	@Override
	public void run() {
		ThreadUtils.sleepMs(RandomUtils.nextInt(5000, 10000));
		super.run();
	}
}
