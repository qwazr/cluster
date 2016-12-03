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

import java.util.Arrays;
import java.util.List;

public class MastersTest extends AbstractMultiTests {

	private static final List<String> MASTERS = Arrays.asList("localhost:9092", "localhost:9093");

	@Override
	protected void startServers() throws Exception {
		int port = 9092;
		master1 = new ClusterTestServer(MASTERS, port++, null, null, GROUP_MASTER);
		master2 = new ClusterTestServer(MASTERS, port++, null, null, GROUP_MASTER);
		front1 = new ClusterTestServer(MASTERS, port++, null, null, GROUP_FRONT);
		front2 = new ClusterTestServer(MASTERS, port++, null, null, GROUP_FRONT);
		front3 = new ClusterTestServer(MASTERS, port++, null, null, GROUP_FRONT);
	}

}
