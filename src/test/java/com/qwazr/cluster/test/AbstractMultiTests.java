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

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class AbstractMultiTests {

	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMultiTests.class);

	static TestServer master1;
	static TestServer master2;
	static TestServer front1;
	static TestServer front2;
	static TestServer front3;

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
		for (int i = 0; i < 30; i++) {
			int found = 0;
			try {
				for (TestServer server : TestServer.servers) {
					if (server.client.getActiveNodesByService("cluster", null).size() == TestServer.servers.size())
						found++;
				}
				if (found == TestServer.servers.size())
					return;
			} catch (WebApplicationException e) {
			}
			LOGGER.info(found + " / " + TestServer.servers.size());
			Thread.sleep(1000);
		}
		Assert.fail();
	}

	@AfterClass
	public static void after() {
		TestServer.closeAll();
	}
}
