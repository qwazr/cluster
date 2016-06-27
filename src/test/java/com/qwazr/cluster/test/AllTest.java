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
import com.qwazr.cluster.service.ClusterServiceStatusJson;
import com.qwazr.cluster.service.ClusterSingleClient;
import com.qwazr.cluster.service.ClusterStatusJson;
import com.qwazr.utils.server.RemoteService;
import com.qwazr.utils.server.ServerConfiguration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AllTest {

	private final String CLIENT_ADDRESS = "http://localhost:9091";

	private static final Logger logger = LoggerFactory.getLogger(AllTest.class);

	private ClusterSingleClient getClusterClient() throws URISyntaxException {
		return new ClusterSingleClient(new RemoteService(CLIENT_ADDRESS));
	}

	private final static String[] GROUPS = {"group1", "group2"};

	@BeforeClass
	public static void startServer() throws Exception {
		final File dataDir = Files.createTempDir();
		System.setProperty("QWAZR_DATA", dataDir.getAbsolutePath());
		System.setProperty("PUBLIC_ADDR", "localhost");
		System.setProperty("LISTEN_ADDR", "localhost");
		//System.setProperty("UDP_ADDRESS", UdpServerThread.DEFAULT_MULTICAST);
		System.setProperty("QWAZR_NODES", "localhost:9091");
		ClusterServer.start(Arrays.asList(GROUPS));
	}

	/**
	 * We wait 30 seconds until the service is visible as active.
	 *
	 * @throws URISyntaxException
	 * @throws InterruptedException
	 */
	@Test
	public void test15_check_service_activation() throws URISyntaxException, InterruptedException {
		int count = 0;
		int activated_services_count = 0;
		int activated_groups_count = 0;
		while (count++ < 20) {
			activated_services_count = 0;
			activated_groups_count = 0;

			logger.info("Check service activation: " + count);
			ClusterServiceStatusJson result = getClusterClient().getServiceStatus("cluster", null);
			if (result.active != null && result.active.contains(CLIENT_ADDRESS))
				activated_services_count++;
			for (String group : GROUPS) {
				logger.info("Check group activation: " + count);
				ClusterServiceStatusJson resultGroup = getClusterClient().getServiceStatus("cluster", group);
				if (resultGroup.active != null && resultGroup.active.contains(CLIENT_ADDRESS))
					activated_groups_count++;
			}
			if (activated_services_count == 1 && activated_groups_count == GROUPS.length) {
				logger.info("Check activation succeed");
				break;
			}
			Thread.sleep(5000);
		}
		Assert.assertEquals(1, activated_services_count);
		Assert.assertEquals(GROUPS.length, activated_groups_count);
	}

	@Test
	public void test20_get_node_list() throws URISyntaxException {
		Set<String> result = getClusterClient().getNodes();
		Assert.assertNotNull(result);
		Assert.assertEquals(1, result.size());
	}

	@Test
	public void test22_get_active_list_by_service() throws URISyntaxException {
		TreeSet<String> result = getClusterClient().getActiveNodesByService("cluster", null);
		Assert.assertNotNull(result);
		Assert.assertEquals(1, result.size());
		Assert.assertEquals(CLIENT_ADDRESS, result.first());
		for (String group : GROUPS) {
			TreeSet<String> resultGroup = getClusterClient().getActiveNodesByService("cluster", group);
			Assert.assertNotNull(resultGroup);
			Assert.assertEquals(1, resultGroup.size());
			Assert.assertEquals(CLIENT_ADDRESS, resultGroup.first());
		}
	}

	@Test
	public void test25_active_random_service() throws URISyntaxException {
		String result = getClusterClient().getActiveNodeRandomByService("cluster", null);
		Assert.assertNotNull(result);
		Assert.assertEquals(CLIENT_ADDRESS, result);
		for (String group : GROUPS) {
			String resultGroup = getClusterClient().getActiveNodeRandomByService("cluster", group);
			Assert.assertNotNull(resultGroup);
			Assert.assertEquals(CLIENT_ADDRESS, resultGroup);
		}
	}

	@Test
	public void test30_active_leader() throws URISyntaxException {
		String result = getClusterClient().getActiveNodeLeaderByService("cluster", null);
		Assert.assertNotNull(result);
		Assert.assertEquals(CLIENT_ADDRESS, result);
		for (String group : GROUPS) {
			String resultGroup = getClusterClient().getActiveNodeLeaderByService("cluster", group);
			Assert.assertNotNull(resultGroup);
			Assert.assertEquals(CLIENT_ADDRESS, resultGroup);
		}
	}

	@Test
	public void test35_get_service_map() throws URISyntaxException {
		TreeMap<String, ClusterServiceStatusJson.StatusEnum> result = getClusterClient().getServiceMap(null);
		Assert.assertNotNull(result);
		Assert.assertEquals(1, result.size());
		Assert.assertEquals(ClusterServiceStatusJson.StatusEnum.ok, result.values().iterator().next());
		for (String group : GROUPS) {
			result = getClusterClient().getServiceMap(group);
			Assert.assertNotNull(result);
			Assert.assertEquals(1, result.size());
			Assert.assertEquals(ClusterServiceStatusJson.StatusEnum.ok, result.values().iterator().next());
		}
	}

	@Test
	public void test40_list() throws URISyntaxException {
		ClusterStatusJson status = getClusterClient().list();
		Assert.assertNotNull(status);
		Assert.assertEquals(1, status.active_nodes.size());
		Assert.assertEquals(CLIENT_ADDRESS, status.active_nodes.values().iterator().next().address);
	}
}
