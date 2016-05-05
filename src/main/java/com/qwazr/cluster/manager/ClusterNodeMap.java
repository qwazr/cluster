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

import java.util.*;

public class ClusterNodeMap {

	private final ReadWriteLock readWriteLock = new ReadWriteLock();

	private final HashMap<String, ClusterNode> nodesMap;
	private final HashMap<String, HashSet<String>> groupsMap;
	private final HashMap<String, HashSet<String>> servicesMap;

	private volatile HashMap<String, ClusterNodeJson> cacheNodesMap;
	private volatile TreeMap<String, TreeSet<String>> cacheGroupsMap;
	private volatile TreeMap<String, TreeSet<String>> cacheServicesMap;

	ClusterNodeMap() {
		nodesMap = new HashMap<>();
		groupsMap = new HashMap<>();
		servicesMap = new HashMap<>();
		buildCaches();
	}

	private void buildCacheGroupsMap() {
		final TreeMap<String, TreeSet<String>> newMap = new TreeMap<>();
		groupsMap.forEach((key, nodes) -> newMap.put(key, new TreeSet<>(nodes)));
		cacheGroupsMap = newMap;
	}

	private void buildCacheServicesMap() {
		final TreeMap<String, TreeSet<String>> newMap = new TreeMap<>();
		servicesMap.forEach((key, nodes) -> newMap.put(key, new TreeSet<>(nodes)));
		cacheServicesMap = newMap;
	}

	private void buildCacheNodesList() {
		final HashMap<String, ClusterNodeJson> newMap = new HashMap<>();
		nodesMap.forEach((address, clusterNode) -> newMap
				.put(address, new ClusterNodeJson(address, clusterNode.groups, clusterNode.getServices())));
		cacheNodesMap = newMap;
	}

	private void buildCaches() {
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
		final TreeSet<String> groupSet = cacheGroupsMap.get(group);
		final TreeSet<String> serviceSet = cacheServicesMap.get(service);
		final TreeSet<String> nodes = new TreeSet<>();
		if (groupSet == null || serviceSet == null)
			return nodes;
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
	final Map<String, ClusterNodeJson> getNodesMap() {
		return cacheNodesMap;
	}

	private static void registerSet(final String key, final HashMap<String, HashSet<String>> nodesMap,
			final String address) {
		HashSet<String> nodeSet = nodesMap.get(key);
		if (nodeSet == null) {
			nodeSet = new HashSet<>();
			nodesMap.put(key, nodeSet);
		} else if (nodeSet.contains(address))
			return;
		nodeSet.add(address);
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

	private ClusterNode registerNode(final String address) {
		ClusterNode node = nodesMap.get(address);
		if (node == null) {
			node = new ClusterNode();
			nodesMap.put(address, node);
		}
		return node;
	}

	/**
	 * Insert or update a node
	 *
	 * @param address the node to register
	 */
	final void register(final String address) {
		if (address == null)
			return;
		readWriteLock.w.lock();
		try {
			registerNode(address);
			buildCacheNodesList();
		} finally {
			readWriteLock.w.unlock();
		}
	}

	final void register(final ClusterProtocol.AddressResponse response) {
		if (response == null)
			return;
		registerNode(response.getAddress());
	}

	public void registerGroup(ClusterProtocol.NameResponse response) {
		if (response == null)
			return;
		final String address = response.getAddress();
		if (address == null)
			return;
		final String group = response.getName();
		if (group == null)
			return;
		readWriteLock.w.lock();
		try {
			registerNode(address).registerGroup(group);
			registerSet(group, groupsMap, address);
			buildCacheNodesList();
			buildCacheGroupsMap();
		} finally {
			readWriteLock.w.unlock();
		}
	}

	public void registerService(ClusterProtocol.NameResponse response) {
		if (response == null)
			return;
		final String address = response.getAddress();
		if (address == null)
			return;
		final String service = response.getName();
		if (service == null)
			return;
		readWriteLock.w.lock();
		try {
			registerNode(address).registerService(service);
			registerSet(service, servicesMap, address);
			buildCacheNodesList();
			buildCacheServicesMap();
		} finally {
			readWriteLock.w.unlock();
		}
	}

	/**
	 * Unregister the node
	 *
	 * @param address the node to unregister
	 */
	private void unregisterAll(final String address) {
		unregisterSet(groupsMap, address);
		unregisterSet(servicesMap, address);
		nodesMap.remove(address);
		buildCaches();
	}

	/**
	 * Remove the node
	 *
	 * @param response the node to unregister
	 */
	final void unregister(final ClusterProtocol.AddressResponse response) {
		if (response == null)
			return;
		unregisterAll(response.getAddress());
	}

}
