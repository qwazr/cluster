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

import com.qwazr.cluster.service.ClusterNodeJson;
import com.qwazr.utils.LockUtils.ReadWriteLock;
import com.qwazr.utils.StringUtils;
import com.qwazr.utils.server.ServerException;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.BiConsumer;

public class ClusterNodeMap {

	private final ReadWriteLock readWriteLock = new ReadWriteLock();

	private final HashMap<String, ClusterNode> nodesMap;
	private final HashMap<String, HashMap<String, ClusterNodeSet>> nodesByGroupServiceMap;
	private volatile HashMap<String, HashMap<String, ClusterNodeSet>> cacheNodesByGroupServiceMap;
	private volatile List<ClusterNode> cacheNodesList;

	ClusterNodeMap() {
		nodesMap = new HashMap<String, ClusterNode>();
		nodesByGroupServiceMap = new HashMap<String, HashMap<String, ClusterNodeSet>>();
		buildCacheNodesByServiceMap();
		buildCacheNodesList();
	}

	private synchronized void buildCacheNodesByServiceMap() {
		final HashMap<String, HashMap<String, ClusterNodeSet>> newMap = new HashMap<String, HashMap<String, ClusterNodeSet>>();
		nodesByGroupServiceMap.forEach(new BiConsumer<String, HashMap<String, ClusterNodeSet>>() {
			@Override
			public void accept(String group, HashMap<String, ClusterNodeSet> map) {
				newMap.put(group, new HashMap<String, ClusterNodeSet>(map));
			}
		});
		cacheNodesByGroupServiceMap = newMap;
	}

	private synchronized void buildCacheNodesList() {
		cacheNodesList = new ArrayList<ClusterNode>(nodesMap.values());
	}

	/**
	 * @param service the name of the service
	 * @param group   the name of the group
	 * @return a list of nodes for the given service
	 */
	ClusterNodeSet getNodeSetByService(String service, String group) {
		HashMap<String, ClusterNodeSet> groupMap = cacheNodesByGroupServiceMap
						.get(group == null ? StringUtils.EMPTY : group);
		if (groupMap == null)
			return null;
		return groupMap.get(service);
	}

	/**
	 * @return a list which contains the nodes
	 */
	List<ClusterNode> getNodeList() {
		return cacheNodesList;
	}

	/**
	 * Register the node for the given service
	 *
	 * @param node     the node to register
	 * @param nodesMap the node map
	 */
	private static void register(ClusterNode node, HashMap<String, ClusterNodeSet> nodesMap, String service) {
		ClusterNodeSet nodeSet = nodesMap.get(service);
		if (nodeSet == null) {
			nodeSet = new ClusterNodeSet();
			nodesMap.put(service, nodeSet);
		}
		nodeSet.insert(node);
	}

	private void register(ClusterNode node, String group, String service) {
		HashMap<String, ClusterNodeSet> nodesMap = nodesByGroupServiceMap.get(group);
		if (nodesMap == null) {
			nodesMap = new HashMap<String, ClusterNodeSet>();
			nodesByGroupServiceMap.put(group, nodesMap);
		}
		register(node, nodesMap, service);
	}

	private void register(ClusterNode node, String service) {
		register(node, StringUtils.EMPTY, service);
		if (node.groups == null)
			return;
		for (String group : node.groups)
			register(node, group, service);
	}

	/**
	 * Unregister the node from a given service
	 *
	 * @param node     the node to unregister
	 * @param nodesMap the node map
	 */
	private static void unregister(ClusterNode node, HashMap<String, ClusterNodeSet> nodesMap, String service) {
		ClusterNodeSet nodeSet = nodesMap.get(service);
		if (nodeSet == null)
			return;
		nodeSet.remove(node);
		if (nodeSet.isEmpty())
			nodesMap.remove(service);
	}

	private void unregister(ClusterNode node, String group, String service) {
		HashMap<String, ClusterNodeSet> nodesMap = nodesByGroupServiceMap.get(group);
		if (nodesMap == null)
			return;
		unregister(node, nodesMap, service);
		if (nodesMap.isEmpty())
			nodesByGroupServiceMap.remove(group);
	}

	private void unregister(ClusterNode node, String service) {
		unregister(node, StringUtils.EMPTY, service);
		if (node.groups == null)
			return;
		for (String group : node.groups)
			unregister(node, group, service);
	}

	/**
	 * Update the services of an existing node
	 *
	 * @param node    the node to update
	 * @param newNode The new node parameters
	 */
	private void update(ClusterNode node, ClusterNodeJson newNode) {
		synchronized (nodesByGroupServiceMap) {
			if (node.services != null)
				for (String service : node.services)
					unregister(node, service);
			node.setServices(newNode.services);
			node.setGroups(newNode.groups);
			if (node.services != null)
				for (String service : node.services)
					register(node, service);
			buildCacheNodesByServiceMap();
		}
	}

	/**
	 * Register the services for a given node
	 *
	 * @param node the node to register
	 */
	private void register(ClusterNode node) {
		if (node.services == null)
			return;
		synchronized (nodesByGroupServiceMap) {
			for (String service : node.services)
				register(node, service);
			buildCacheNodesByServiceMap();
		}
	}

	/**
	 * Unregister the services for the given node.
	 *
	 * @param node the node to unregister
	 */
	private void unregister(ClusterNode node) {
		if (node.services == null)
			return;
		synchronized (nodesByGroupServiceMap) {
			for (String service : node.services)
				unregister(node, service);
			buildCacheNodesByServiceMap();
		}
	}

	/**
	 * Insert or update a node
	 *
	 * @param clusterNodeJson the node to register
	 * @return the node record
	 * @throws URISyntaxException
	 * @throws ServerException
	 */
	ClusterNode upsert(ClusterNodeJson clusterNodeJson) throws URISyntaxException, ServerException {

		ClusterNode newNode = new ClusterNode(clusterNodeJson);

		// Let's check if we already have the node
		readWriteLock.r.lock();
		try {
			ClusterNode oldNode = nodesMap.get(newNode.address);
			if (oldNode != null) {
				update(oldNode, clusterNodeJson);
				return oldNode;
			}
		} finally {
			readWriteLock.r.unlock();
		}

		// It's a new one, we insert it
		readWriteLock.w.lock();
		try {
			nodesMap.put(newNode.address, newNode);
			register(newNode);
			buildCacheNodesList();
			return newNode;
		} finally {
			readWriteLock.w.unlock();
		}
	}

	/**
	 * Remove the node
	 *
	 * @param address the address of the node, in the form host:port
	 * @return the removed node
	 * @throws URISyntaxException
	 */
	ClusterNode remove(String address) throws URISyntaxException {

		// Let's check if we know the node
		readWriteLock.r.lock();
		try {
			// We do not know the node, nothing to do
			if (!nodesMap.containsKey(address))
				return null;
		} finally {
			readWriteLock.r.unlock();
		}

		// Ok, let's remove the node
		readWriteLock.w.lock();
		try {
			// Removed from the node map
			ClusterNode node = nodesMap.remove(address);
			if (node == null)
				return null;
			// Removed from the service map
			unregister(node);
			buildCacheNodesList();
			return node;
		} finally {
			readWriteLock.w.unlock();
		}
	}

	void status(ClusterNode node) {
		if (node == null)
			return;
		readWriteLock.r.lock();
		try {
			register(node);
		} finally {
			readWriteLock.r.unlock();
		}
	}

	HashMap<String, ClusterNodeSet> getServicesMap(String group) {
		return cacheNodesByGroupServiceMap.get(group == null ? StringUtils.EMPTY : group);
	}

}
