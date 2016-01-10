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

import com.qwazr.utils.server.ServerException;
import com.qwazr.utils.threads.PeriodicThread;
import com.qwazr.utils.threads.ThreadUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterMonitoringThread extends PeriodicThread {

	private static final Logger logger = LoggerFactory.getLogger(ClusterMonitoringThread.class);

	private final RequestConfig requestConfig;
	private final CloseableHttpAsyncClient httpclient;

	ClusterMonitoringThread(int monitoring_period_seconds) {
		super("Nodes monitoring", monitoring_period_seconds);
		setDaemon(true);
		requestConfig = RequestConfig.custom().setSocketTimeout(monitoring_period).setConnectTimeout(monitoring_period)
				.build();
		httpclient = HttpAsyncClients.custom().setDefaultRequestConfig(requestConfig).build();
		httpclient.start();
		start();
	}

	@Override
	public void runner() {
		try {
			for (ClusterNode clusterNode : ClusterManager.INSTANCE.getNodeList())
				clusterNode.startCheck(httpclient);
		} catch (ServerException e) {
			logger.error(e.getMessage(), e);
		}
	}

	@Override
	public void run() {
		ThreadUtils.sleepMs(RandomUtils.nextInt(10000, 15000));
		super.run();
	}
}
