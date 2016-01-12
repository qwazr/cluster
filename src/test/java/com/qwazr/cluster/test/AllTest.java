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

import com.qwazr.cluster.client.ClusterSingleClient;
import com.qwazr.cluster.service.ClusterNodeJson;
import com.qwazr.cluster.service.ClusterNodeStatusJson;
import com.qwazr.cluster.service.ClusterServiceStatusJson;
import com.qwazr.cluster.service.ClusterServiceStatusJson.StatusEnum;
import com.qwazr.cluster.service.ClusterStatusJson;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AllTest {

	private final String CLIENT_ADDRESS = "http://localhost:9091";

	private final int CLIENT_TIMEOUT = 60000;

	private final String[] SERVICES = { "job", "search" };

	private final String[] GROUPS = { "group1", "group2", "group3" };

	private static final Logger logger = LoggerFactory.getLogger(AllTest.class);

	private ClusterSingleClient getClusterClient() throws URISyntaxException {
		return new ClusterSingleClient(CLIENT_ADDRESS, CLIENT_TIMEOUT);
	}

	@Test
	public void test01_list() throws URISyntaxException {
		ClusterStatusJson result = getClusterClient().list();
		Assert.assertNotNull(result);
		Assert.assertTrue(result.is_master);
		Assert.assertNotNull(result.masters);
		Assert.assertTrue(ArrayUtils.contains(result.masters, CLIENT_ADDRESS));
	}

	private ClusterNodeJson getClusterNodeJson() {
		HashSet<String> serviceSet = new HashSet<String>(Arrays.asList(SERVICES));
		HashSet<String> groupSet = new HashSet<String>(Arrays.asList(GROUPS));
		return new ClusterNodeJson(CLIENT_ADDRESS, serviceSet, groupSet);
	}

	@Test
	public void test10_register_two_services() throws URISyntaxException {
		ClusterNodeStatusJson result = getClusterClient().register(getClusterNodeJson());
		Assert.assertNotNull(result);
		Assert.assertNull(result.error, result.error);
	}

	@Test
	public void test11_check_register() throws URISyntaxException {
		ClusterNodeStatusJson result = getClusterClient().register(getClusterNodeJson());
		Assert.assertNotNull(result);
		Assert.assertNull(result.error, result.error);
	}

	@Test
	public void test12_list_non_empty() throws URISyntaxException {
		ClusterStatusJson result = getClusterClient().list();
		Assert.assertNotNull(result);
		Assert.assertNotNull(result.services);
		Assert.assertEquals(SERVICES.length, result.services.size());
		Assert.assertNotNull(result.active_nodes);
		Assert.assertNotNull(result.inactive_nodes);
		Assert.assertTrue(result.active_nodes.size() == 1 || result.inactive_nodes.size() == 1);
	}

	private static boolean checkClusterKeyStatus(ClusterServiceStatusJson result) {
		Assert.assertNotNull(result);
		Assert.assertTrue(result.inactive_count == 1 || result.active_count == 1);
		Assert.assertNotNull(result.status);
		if (result.active_count == 1) {
			Assert.assertNotNull(result.active);
			Assert.assertEquals(1, result.active.length);
			Assert.assertEquals(StatusEnum.ok, result.status);
			return true;
		} else {
			Assert.assertNotNull(result.inactive);
			Assert.assertEquals(1, result.inactive.size());
			Assert.assertEquals(StatusEnum.failure, result.status);
			return false;
		}
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
			for (String service : SERVICES) {
				logger.info("Check service activation: " + count);
				ClusterServiceStatusJson result = getClusterClient().getServiceStatus(service, null);
				if (checkClusterKeyStatus(result))
					activated_services_count++;
				for (String group : GROUPS) {
					logger.info("Check group activation: " + count);
					ClusterServiceStatusJson resultGroup = getClusterClient().getServiceStatus(service, group);
					if (checkClusterKeyStatus(resultGroup))
						activated_groups_count++;
				}
			}
			if (activated_services_count == SERVICES.length
							&& activated_groups_count == GROUPS.length * SERVICES.length) {
				logger.info("Check activation succeed");
				break;
			}
			Thread.sleep(5000);
		}
		Assert.assertEquals(SERVICES.length, activated_services_count);
		Assert.assertEquals(GROUPS.length * SERVICES.length, activated_groups_count);
	}

	@Test
	public void test20_get_node_list() throws URISyntaxException {
		Map<String, ClusterNodeJson> result = getClusterClient().getNodes();
		Assert.assertNotNull(result);
		Assert.assertEquals(1, result.size());
	}

	@Test
	public void test22_get_active_list_by_service() throws URISyntaxException {
		for (String service : SERVICES) {
			String[] result = getClusterClient().getActiveNodesByService(service, null);
			Assert.assertNotNull(result);
			Assert.assertEquals(1, result.length);
			Assert.assertEquals(CLIENT_ADDRESS, result[0]);
			for (String group : GROUPS) {
				String[] resultGroup = getClusterClient().getActiveNodesByService(service, group);
				Assert.assertNotNull(resultGroup);
				Assert.assertEquals(1, resultGroup.length);
				Assert.assertEquals(CLIENT_ADDRESS, resultGroup[0]);
			}
		}
	}

	@Test
	public void test25_active_random_service() throws URISyntaxException {
		for (String service : SERVICES) {
			String result = getClusterClient().getActiveNodeRandomByService(service, null);
			Assert.assertNotNull(result);
			Assert.assertEquals(CLIENT_ADDRESS, result);
			for (String group : GROUPS) {
				String resultGroup = getClusterClient().getActiveNodeRandomByService(service, group);
				Assert.assertNotNull(resultGroup);
				Assert.assertEquals(CLIENT_ADDRESS, resultGroup);
			}
		}
	}

	@Test
	public void test30_check_unregister() throws URISyntaxException {
		Response response = getClusterClient().unregister(CLIENT_ADDRESS);
		Assert.assertNotNull(response);
		Assert.assertEquals(200, response.getStatus());
	}

	@Test
	public void test32_list_is_empty() throws URISyntaxException {
		ClusterStatusJson result = getClusterClient().list();
		Assert.assertNotNull(result);
		Assert.assertNotNull(result.services);
		Assert.assertNotNull(result.active_nodes);
		Assert.assertNotNull(result.inactive_nodes);
		Assert.assertEquals(result.services.size(), 0);
		Assert.assertEquals(result.active_nodes.size(), 0);
		Assert.assertEquals(result.inactive_nodes.size(), 0);
	}
}
