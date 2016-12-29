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

import com.qwazr.cluster.ClusterServiceBuilder;
import com.qwazr.server.RemoteService;
import org.apache.commons.lang3.NotImplementedException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URISyntaxException;

public class ServiceBuilderTest {

	static ClusterServiceBuilder builder;

	@BeforeClass
	public static void before() {
		builder = new ClusterServiceBuilder();
	}

	@Test
	public void local() {
		Assert.assertNull(builder.local());
	}

	@Test
	public void remote() throws URISyntaxException {
		Assert.assertNotNull(builder.remote(new RemoteService("http://localhost:9091")));
	}

	@Test
	public void remotes() {
		try {
			builder.remotes(null);
			Assert.fail("NotImplementedException not thrown");
		} catch (NotImplementedException e) {
			//OK
		}
	}
}
