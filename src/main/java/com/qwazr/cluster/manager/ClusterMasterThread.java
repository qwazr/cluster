/**
 * Copyright 2014-2015 Emmanuel Keller / QWAZR
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
package com.qwazr.cluster.manager;

import com.qwazr.utils.threads.PeriodicThread;

public class ClusterMasterThread extends PeriodicThread {

	ClusterMasterThread(int monitoring_period_seconds) {
		super("Master sync", monitoring_period_seconds);
		setDaemon(true);
		start();
	}

	@Override
	protected void runner() {
		ClusterManager.INSTANCE.loadNodesFromOtherMaster();
	}
}
