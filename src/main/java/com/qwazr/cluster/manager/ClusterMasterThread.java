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

import com.qwazr.utils.threads.PeriodicThread;
import org.apache.commons.lang3.RandomUtils;

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

	@Override
	public void run() {
		sleepMs(RandomUtils.nextInt(0, 5000));
		super.run();
	}
}
