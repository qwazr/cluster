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
import com.qwazr.utils.server.ServerException;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ClusterNodeMap {

	private final ReadWriteLock readWriteLock = new ReadWriteLock();

	private final HashMap<String, ClusterNode> nodesMap;
	private final HashMap<String, ClusterNodeSet> nodesByServiceMap;
	private final HashMap<String, ClusterNodeSet> nodesByGroupMap;

	private volatile HashMap<String, ClusterNodeSet> cacheNodesByServiceMap;
	private volatile HashMap<String, ClusterNodeSet> cacheNodesByGroupMap;
	private volatile List<ClusterNode> cacheNodesList;

	ClusterNodeMap() {
		nodesMap = new HashMap<String, ClusterNode>();
		nodesByServiceMap = new HashMap<String, ClusterNodeSet>();
		nodesByGroupMap = new HashMap<String, ClusterNodeSet>();
		buildCacheNodesByServiceMap();
		buildCacheNodesByGroupMap();
		buildCacheNodesList();
	}

	private synchronized void buildCacheNodesByServiceMap() {
		cacheNodesByServiceMap = new HashMap<String, ClusterNodeSet>(nodesByServiceMap);
	}

	private synchronized void buildCacheNodesByGroupMap() {
		cacheNodesByGroupMap = new HashMap<String, ClusterNodeSet>(nodesByGroupMap);
	}

	private void buildCacheNodesList() {
		cacheNodesList = new ArrayList<ClusterNode>(nodesMap.values());
	}

	/**
	 * @param service
	 * @return a list of nodes for the given service
	 */
	ClusterNodeSet getNodeSetByService(String service) {
		return cacheNodesByServiceMap.get(service);
	}

	/**
	 * @param group
	 * @return a set of nodes for the given group
	 */
	ClusterNodeSet getNodeSetByGroup(String group) {
		return cacheNodesByGroupMap.get(group);
	}

	/**
	 * @return a list which contains the nodes
	 */
	List<ClusterNode> getNodeList() {
		return cacheNodesList;
	}

	/**
	 * Register the node for the given key
	 *
	 * @param node     the node to register
	 * @param nodesMap the node map
	 */
	private static void register(ClusterNode node, HashMap<String, ClusterNodeSet> nodesMap, String key) {
		ClusterNodeSet nodeSet = nodesMap.get(key);
		if (nodeSet == null) {
			nodeSet = new ClusterNodeSet();
			nodesMap.put(key, nodeSet);
		}
		nodeSet.insert(node);
	}

	/**
	 * Unregister the node from a given key
	 *
	 * @param node     the node to unregister
	 * @param nodesMap the node map
	 */
	private static void unregister(ClusterNode node, HashMap<String, ClusterNodeSet> nodesMap, String key) {
		ClusterNodeSet nodeSet = nodesMap.get(key);
		if (nodeSet == null)
			return;
		nodeSet.remove(node);
		if (nodeSet.isEmpty())
			nodesMap.remove(key);
	}

	/**
	 * Update the services of an existing node
	 *
	 * @param node    the node to update
	 * @param newNode The new node parameters
	 */
	private void update(ClusterNode node, ClusterNodeJson newNode) {
		synchronized (nodesByServiceMap) {
			if (node.services != null)
				for (String service : node.services)
					unregister(node, nodesByServiceMap, service);
			node.setServices(newNode.services);
			if (node.services != null)
				for (String service : node.services)
					register(node, nodesByServiceMap, service);
			buildCacheNodesByServiceMap();
		}
		synchronized (nodesByGroupMap) {
			if (node.groups != null)
				for (String group : node.groups)
					unregister(node, nodesByGroupMap, group);
			node.setGroups(newNode.groups);
			if (node.groups != null)
				for (String group : node.groups)
					register(node, nodesByGroupMap, group);
			buildCacheNodesByGroupMap();
		}
	}

	/**
	 * Register the services for a given node
	 *
	 * @param node the node to register
	 */
	private void register(ClusterNode node) {
		if (node.services != null) {
			synchronized (nodesByServiceMap) {
				for (String service : node.services)
					register(node, nodesByServiceMap, service);
				buildCacheNodesByServiceMap();
			}
		}
		if (node.groups != null) {
			synchronized (nodesByGroupMap) {
				for (String group : node.groups)
					register(node, nodesByGroupMap, group);
				buildCacheNodesByGroupMap();
			}
		}
	}

	/**
	 * Unregister the services for the given node.
	 *
	 * @param node the node to unregister
	 */
	private void unregister(ClusterNode node) {
		if (node.services != null) {
			synchronized (nodesByServiceMap) {
				for (String service : node.services)
					unregister(node, nodesByServiceMap, service);
				buildCacheNodesByServiceMap();
			}
		}
		if (node.groups != null) {
			synchronized (nodesByGroupMap) {
				for (String group : node.groups)
					unregister(node, nodesByGroupMap, group);
				buildCacheNodesByGroupMap();
			}
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

	HashMap<String, ClusterNodeSet> getServicesMap() {
		return cacheNodesByServiceMap;
	}

	HashMap<String, ClusterNodeSet> getGroupsMap() {
		return cacheNodesByGroupMap;
	}

}
