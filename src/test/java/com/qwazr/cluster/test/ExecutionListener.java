/**
 * Copyright 2014-2015 OpenSearchServer Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qwazr.cluster.test;

import java.io.File;

import org.junit.runner.Description;
import org.junit.runner.notification.RunListener;

import com.qwazr.cluster.ClusterServer;

public class ExecutionListener extends RunListener {

	private final String DATADIR_PATH = "src/test/resources/com/opensearchserver/cluster/test/datadir";

	@Override
	public void testRunStarted(Description description) throws Exception {
		final File dataDir = new File(DATADIR_PATH);
		final String parameter = "-d" + dataDir.getAbsolutePath();
		final String[] parameters = { parameter };
		ClusterServer.main(parameters);
	}
}
