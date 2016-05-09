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

import com.qwazr.utils.LockUtils.ReadWriteLock;
import com.qwazr.utils.StringUtils;

import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.*;

public class ClusterNodeMap {

	private final InetSocketAddress myAddress;

	private final ReadWriteLock readWriteLock = new ReadWriteLock();

	private final HashMap<String, ClusterNode> nodesMap;
	private final HashMap<String, HashSet<String>> groupsMap;
	private final HashMap<String, HashSet<String>> servicesMap;

	private volatile HashMap<String, ClusterNode> cacheNodesMap;
	private volatile Set<InetSocketAddress> cacheFullNodesAddresses;
	private volatile Set<InetSocketAddress> cacheExternalNodesAddresses;
	private volatile TreeMap<String, TreeSet<String>> cacheGroupsMap;
	private volatile TreeMap<String, TreeSet<String>> cacheServicesMap;

	ClusterNodeMap(final InetSocketAddress myAddress) {
		this.myAddress = myAddress;
		nodesMap = new HashMap<>();
		groupsMap = new HashMap<>();
		servicesMap = new HashMap<>();
		buildCaches();
	}

	private synchronized void buildCacheGroupsMap() {
		final TreeMap<String, TreeSet<String>> newMap = new TreeMap<>();
		groupsMap.forEach((key, nodes) -> newMap.put(key, new TreeSet<>(nodes)));
		cacheGroupsMap = newMap;
	}

	private synchronized void buildCacheServicesMap() {
		final TreeMap<String, TreeSet<String>> newMap = new TreeMap<>();
		servicesMap.forEach((key, nodes) -> newMap.put(key, new TreeSet<>(nodes)));
		cacheServicesMap = newMap;
	}

	private synchronized void buildCacheNodesList() {
		final HashSet<InetSocketAddress> newExternalSet = new HashSet();
		final HashSet<InetSocketAddress> newFullSet = new HashSet();
		final HashMap<String, ClusterNode> newMap = new HashMap<>();
		nodesMap.forEach((address, clusterNode) -> {
			if (!myAddress.equals(clusterNode.address.address))
				newExternalSet.add(clusterNode.address.address);
			newFullSet.add(clusterNode.address.address);
			newMap.put(address, clusterNode);
		});
		cacheNodesMap = newMap;
		cacheFullNodesAddresses = newFullSet;
		cacheExternalNodesAddresses = newExternalSet;
	}

	private synchronized void buildCaches() {
		buildCacheGroupsMap();
		buildCacheServicesMap();
		buildCacheNodesList();
	}

	/**
	 * @param group   the name of the group
	 * @param service the name of the service
	 * @return a set of nodes for the given group and service
	 */
	final TreeSet<String> getGroupService(final String group, final String service) {
		final TreeSet<String> groupSet = group == null ? null : cacheGroupsMap.get(group);
		final TreeSet<String> serviceSet = service == null ? null : cacheServicesMap.get(service);
		final TreeSet<String> nodes = new TreeSet<>();
		if (groupSet == null || serviceSet == null)
			return nodes;
		if (groupSet == null) {
			nodes.addAll(serviceSet);
			return nodes;
		}
		groupSet.forEach(node -> {
			if (serviceSet.contains(node))
				nodes.add(node);
		});
		return nodes;
	}

	private static TreeSet<String> getNodes(final String key, final TreeMap<String, TreeSet<String>> nodesMap) {
		final TreeSet<String> nodeSet = nodesMap.get(key);
		final TreeSet<String> nodes = new TreeSet<>();
		if (nodeSet != null)
			nodes.addAll(nodeSet);
		return nodes;
	}

	final TreeSet<String> getByGroup(final String group) {
		return getNodes(group, cacheGroupsMap);
	}

	final TreeSet<String> getByService(final String service) {
		return getNodes(service, cacheServicesMap);
	}

	final TreeMap<String, TreeSet<String>> getGroups() {
		return cacheGroupsMap;
	}

	final TreeMap<String, TreeSet<String>> getServices() {
		return cacheServicesMap;
	}

	/**
	 * @return a map which contains the nodes
	 */
	final Map<String, ClusterNode> getNodesMap() {
		return cacheNodesMap;
	}

	final Set<InetSocketAddress> getExternalNodeAddresses() {
		return cacheExternalNodesAddresses;
	}

	final Set<InetSocketAddress> getFullNodeAddresses() {
		return cacheFullNodesAddresses;
	}

	private static void registerSet(final Collection<String> keys, final HashMap<String, HashSet<String>> nodesMap,
			final String address) {
		for (String key : keys) {
			HashSet<String> nodeSet = nodesMap.get(key);
			if (nodeSet == null) {
				nodeSet = new HashSet<>();
				nodesMap.put(key, nodeSet);
			} else if (nodeSet.contains(address))
				return;
			nodeSet.add(address);
		}
	}

	private static void unregisterSet(final HashMap<String, HashSet<String>> nodesMap, final String address) {
		final List<String> toRemove = new ArrayList<>();
		nodesMap.forEach((key, nodeSet) -> {
			nodeSet.remove(address);
			if (nodeSet.isEmpty())
				toRemove.add(key);

		});
		nodesMap.remove(address);
		toRemove.forEach(nodesMap::remove);
	}

	private ClusterNode put(final String httpAddress, final UUID nodeLiveId) throws URISyntaxException {
		final ClusterNodeAddress clusterNodeAddress = new ClusterNodeAddress(httpAddress);
		ClusterNode node = new ClusterNode(clusterNodeAddress, nodeLiveId);
		nodesMap.put(clusterNodeAddress.httpAddressKey, node);
		return node;
	}

	private ClusterNode registerNode(final String httpAddress, final UUID nodeLiveId) throws URISyntaxException {
		ClusterNode node = nodesMap.get(httpAddress);
		if (node == null)
			return put(httpAddress, nodeLiveId);
		if (nodeLiveId == null)
			return node;
		if (nodeLiveId.equals(node.nodeLiveId))
			return node;
		return put(httpAddress, nodeLiveId);
	}

	/**
	 * Insert or update a node
	 *
	 * @param httpAddress the address of the node
	 * @throws URISyntaxException
	 */
	final ClusterNode register(final String httpAddress) throws URISyntaxException {
		if (httpAddress == null)
			return null;
		readWriteLock.w.lock();
		try {
			final ClusterNode clusterNode = registerNode(httpAddress, null);
			buildCacheNodesList();
			return clusterNode;
		} finally {
			readWriteLock.w.unlock();
		}
	}

	final ClusterNode register(final ClusterProtocol.Full message) throws URISyntaxException {
		if (message == null)
			return null;
		final String address = message.getAddress();
		if (address == null)
			return null;
		readWriteLock.w.lock();
		try {
			final ClusterNode clusterNode = registerNode(message.getAddress(), message.getNodeLiveId());
			unregisterSet(groupsMap, clusterNode.address.httpAddressKey);
			unregisterSet(servicesMap, clusterNode.address.httpAddressKey);
			clusterNode.registerGroups(message.groups);
			clusterNode.registerServices(message.services);
			registerSet(message.groups, groupsMap, address);
			registerSet(message.services, servicesMap, address);
			buildCacheNodesList();
			buildCacheGroupsMap();
			buildCacheServicesMap();
			return clusterNode;
		} finally {
			readWriteLock.w.unlock();
		}
	}

	/**
	 * Unregister the node
	 *
	 * @param address the node to unregister
	 */
	private void unregisterAll(final String address) throws URISyntaxException {
		final ClusterNode node = nodesMap.get(address);
		final ClusterNodeAddress nodeAddress = node != null ? node.address : new ClusterNodeAddress(address);
		unregisterSet(groupsMap, nodeAddress.httpAddressKey);
		unregisterSet(servicesMap, nodeAddress.httpAddressKey);
		nodesMap.remove(nodeAddress.httpAddressKey);
	}

	/**
	 * Remove the node
	 *
	 * @param message the node to unregister
	 */
	final void unregister(final ClusterProtocol.Address message) throws URISyntaxException {
		if (message == null)
			return;
		readWriteLock.w.lock();
		try {
			unregisterAll(message.getAddress());
			buildCaches();
		} finally {
			readWriteLock.w.unlock();
		}
	}

}
