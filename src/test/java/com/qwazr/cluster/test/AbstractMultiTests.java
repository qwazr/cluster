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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import java.util.SortedSet;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class AbstractMultiTests {

	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMultiTests.class);

	static ClusterTestServer master1;
	static ClusterTestServer master2;
	static ClusterTestServer front1;
	static ClusterTestServer front2;
	static ClusterTestServer front3;

	final static String GROUP_MASTER = "master";
	final static String GROUP_FRONT = "front";

	protected abstract void startServers() throws Exception;

	@Test
	public void test00_startInstances() throws Exception {
		startServers();
		Assert.assertNotNull(master1);
		Assert.assertNotNull(master2);
		Assert.assertNotNull(front1);
		Assert.assertNotNull(front2);
		Assert.assertNotNull(front3);
	}

	@Test
	public void test10_findClusters() throws InterruptedException {
		final long end = System.currentTimeMillis() + 120 * 1000 * 2;
		while (System.currentTimeMillis() < end) {
			int found = 0;
			try {
				for (ClusterTestServer server : ClusterTestServer.servers) {
					final SortedSet<String> founds = server.client.getActiveNodesByService("cluster", null);
					if (founds.containsAll(ClusterTestServer.serverAdresses))
						found++;
					else
						LOGGER.warn("Failed on " + server.address + " => " + founds.toString());
				}
				if (found == ClusterTestServer.servers.size())
					return;
			} catch (WebApplicationException e) {
			}
			Thread.sleep(10000);
		}
		Assert.fail();
	}

	@AfterClass
	public static void after() {
		ClusterTestServer.pool.close();
	}
}
