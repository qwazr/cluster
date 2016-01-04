/**
 * Copyright 2014-2016 Emmanuel Keller / QWAZR
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

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.qwazr.utils.LockUtils.ReadWriteLock;
import com.qwazr.utils.server.ServerException;

public class ClusterNodeMap {

	private final ReadWriteLock readWriteLock = new ReadWriteLock();

	private final HashMap<String, ClusterNode> nodesMap;
	private final HashMap<String, ClusterNodeSet> nodesByServiceMap;

	private volatile HashMap<String, ClusterNodeSet> cacheNodesByServiceMap;
	private volatile List<ClusterNode> cacheNodesList;

	ClusterNodeMap() {
		nodesMap = new HashMap<String, ClusterNode>();
		nodesByServiceMap = new HashMap<String, ClusterNodeSet>();
		buildCacheNodesByServiceMap();
		buildCacheNodesList();
	}

	private void buildCacheNodesByServiceMap() {
		cacheNodesByServiceMap = new HashMap<String, ClusterNodeSet>(
				nodesByServiceMap);
	}

	private void buildCacheNodesList() {
		cacheNodesList = new ArrayList<ClusterNode>(nodesMap.values());
	}

	/**
	 * @param service
	 * @return a list of nodes for the given service
	 */
	ClusterNodeSet getNodeSet(String service) {
		return cacheNodesByServiceMap.get(service.intern());
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
	 * @param node
	 *            the node to register
	 * @param service
	 *            any service
	 */
	private void registerService(ClusterNode node, String service) {
		service = service.intern();
		ClusterNodeSet nodeSet = nodesByServiceMap.get(service);
		if (nodeSet == null) {
			nodeSet = new ClusterNodeSet();
			nodesByServiceMap.put(service, nodeSet);
		}
		nodeSet.insert(node);
	}

	/**
	 * Unregister the node from a given service
	 * 
	 * @param node
	 *            the node to unregister
	 * @param service
	 *            any service
	 */
	private void unregisterService(ClusterNode node, String service) {
		service = service.intern();
		ClusterNodeSet nodeSet = nodesByServiceMap.get(service);
		if (nodeSet == null)
			return;
		nodeSet.remove(node);
		if (nodeSet.isEmpty())
			nodesByServiceMap.remove(service);
	}

	/**
	 * Update the services of an existing node
	 * 
	 * @param clusterNode
	 *            the node to update
	 * @param newServices
	 *            The new services
	 */
	private void updateServices(ClusterNode node, Set<String> newServices) {
		synchronized (nodesByServiceMap) {
			if (node.services != null)
				for (String service : node.services)
					unregisterService(node, service);
			node.setServices(newServices);
			if (node.services != null)
				for (String service : node.services)
					registerService(node, service);
			buildCacheNodesByServiceMap();
		}
	}

	/**
	 * Register the services for a given node
	 * 
	 * @param clusterNode
	 *            the node to register
	 */
	private void registerServices(ClusterNode node) {
		if (node.services == null)
			return;
		synchronized (nodesByServiceMap) {
			for (String service : node.services)
				registerService(node, service);
			buildCacheNodesByServiceMap();
		}
	}

	/**
	 * Unregister the services for the given node.
	 * 
	 * @param node
	 *            the node to unregister
	 */
	private void unregisterServices(ClusterNode node) {
		if (node.services == null)
			return;
		synchronized (nodesByServiceMap) {
			for (String service : node.services)
				unregisterService(node, service);
			buildCacheNodesByServiceMap();
		}
	}

	/**
	 * Insert or update a node
	 * 
	 * @param address
	 *            the address of the node
	 * @param services
	 *            the services provided by the node
	 * @return the node record
	 * @throws URISyntaxException
	 * @throws ServerException
	 */
	ClusterNode upsert(String address, Set<String> services)
			throws URISyntaxException, ServerException {

		ClusterNode newNode = new ClusterNode(address, services);

		// Let's check if we already have the node
		readWriteLock.r.lock();
		try {
			ClusterNode oldNode = nodesMap.get(newNode.address);
			if (oldNode != null) {
				updateServices(oldNode, services);
				return oldNode;
			}
		} finally {
			readWriteLock.r.unlock();
		}

		// It's a new one, we insert it
		readWriteLock.w.lock();
		try {
			nodesMap.put(newNode.address, newNode);
			registerServices(newNode);
			buildCacheNodesList();
			return newNode;
		} finally {
			readWriteLock.w.unlock();
		}
	}

	/**
	 * Remove the node
	 * 
	 * @param address
	 *            the address of the node, in the form host:port
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
			unregisterServices(node);
			buildCacheNodesList();
			return node;
		} finally {
			readWriteLock.w.unlock();
		}
	}

	void status(ClusterNode node) {
		if (node == null)
			return;
		if (node.services == null || node.services.isEmpty())
			return;
		readWriteLock.r.lock();
		try {
			registerServices(node);
		} finally {
			readWriteLock.r.unlock();
		}
	}

	HashMap<String, ClusterNodeSet> getServicesMap() {
		return cacheNodesByServiceMap;
	}

}
