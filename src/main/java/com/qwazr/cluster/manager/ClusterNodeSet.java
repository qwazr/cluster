/**
 * Copyright 2015 Emmanuel Keller / QWAZR
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

import java.util.HashMap;

import com.qwazr.utils.LockUtils;

public class ClusterNodeSet {

	private final LockUtils.ReadWriteLock readWriteLock = new LockUtils.ReadWriteLock();

	private final HashMap<String, ClusterNode> activeMap;
	private final HashMap<String, ClusterNode> inactiveMap;

	class Cache {

		final ClusterNode[] activeArray;
		final ClusterNode[] inactiveArray;

		private Cache() {
			this.activeArray = activeMap.values().toArray(
					new ClusterNode[activeMap.size()]);
			this.inactiveArray = inactiveMap.values().toArray(
					new ClusterNode[inactiveMap.size()]);
		}
	}

	private Cache cache;

	ClusterNodeSet() {
		cache = null;
		activeMap = new HashMap<String, ClusterNode>();
		inactiveMap = new HashMap<String, ClusterNode>();
	}

	/**
	 * Move the node to the active set
	 * 
	 * @param node
	 *            The cluster not to insert
	 */
	private void active(ClusterNode node) {
		// We check first if it is not already present in the right list
		readWriteLock.r.lock();
		try {
			if (activeMap.containsKey(node.address))
				return;
		} finally {
			readWriteLock.r.unlock();
		}
		readWriteLock.w.lock();
		try {
			inactiveMap.remove(node.address);
			activeMap.put(node.address, node);
			cache = new Cache();
		} finally {
			readWriteLock.w.unlock();
		}
	}

	/**
	 * Move the node to the inactive set
	 * 
	 * @param node
	 *            The cluster not to insert
	 */
	private void inactive(ClusterNode node) {
		// We check first if it is not already present in the right list
		readWriteLock.r.lock();
		try {
			if (inactiveMap.containsKey(node.address))
				return;
		} finally {
			readWriteLock.r.unlock();
		}
		readWriteLock.w.lock();
		try {
			activeMap.remove(node.address);
			inactiveMap.put(node.address, node);
			cache = new Cache();
		} finally {
			readWriteLock.w.unlock();
		}
	}

	/**
	 * @param node
	 *            The clusterNode to insert
	 */
	void insert(ClusterNode node) {
		if (node.isActive())
			active(node);
		else
			inactive(node);
	}

	/**
	 * 
	 * @param node
	 *            The ClusterNode to remove
	 */
	void remove(ClusterNode node) {
		readWriteLock.w.lock();
		try {
			activeMap.remove(node.address);
			inactiveMap.remove(node.address);
			cache = new Cache();
		} finally {
			readWriteLock.w.unlock();
		}
	}

	/**
	 * @return if the set is empty
	 */
	boolean isEmpty() {
		readWriteLock.r.lock();
		try {
			return activeMap.isEmpty() && inactiveMap.isEmpty();
		} finally {
			readWriteLock.r.unlock();
		}
	}

	/**
	 * @return a cached list of active nodes and inactive nodes
	 */
	Cache getCache() {
		return cache;
	}

}
