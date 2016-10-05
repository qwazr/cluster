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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;

class ClusterNodeMap {

	private static final Logger LOGGER = LoggerFactory.getLogger(ClusterNodeMap.class);

	private final InetSocketAddress myAddress;

	private final ReadWriteLock readWriteLock = new ReadWriteLock();

	private final HashMap<String, ClusterNode> nodesMap;
	private final HashMap<String, HashSet<String>> groupsMap;
	private final HashMap<String, HashSet<String>> servicesMap;

	private volatile Cache cache;

	private class Cache {

		private final HashMap<String, ClusterNode> cacheNodesMap;
		private final Set<SocketAddress> fullNodesAddresses;
		private final Set<SocketAddress> externalNodesAddresses;
		private final TreeMap<String, TreeSet<String>> cacheGroupsMap;
		private final TreeMap<String, TreeSet<String>> cacheServicesMap;

		private Cache() {
			externalNodesAddresses = new HashSet<>();
			fullNodesAddresses = new HashSet<>();
			cacheNodesMap = new HashMap<>();
			nodesMap.forEach((address, clusterNode) -> {
				if (!myAddress.equals(clusterNode.address.address))
					externalNodesAddresses.add(clusterNode.address.address);
				fullNodesAddresses.add(clusterNode.address.address);
				cacheNodesMap.put(address, clusterNode);
			});
			cacheGroupsMap = new TreeMap<>();
			groupsMap.forEach((key, nodes) -> cacheGroupsMap.put(key, new TreeSet<>(nodes)));
			cacheServicesMap = new TreeMap<>();
			servicesMap.forEach((key, nodes) -> cacheServicesMap.put(key, new TreeSet<>(nodes)));
		}
	}

	ClusterNodeMap(final InetSocketAddress myAddress) {
		this.myAddress = myAddress;
		nodesMap = new HashMap<>();
		groupsMap = new HashMap<>();
		servicesMap = new HashMap<>();
		cache = new Cache();
	}

	private final SortedSet<String> EMPTY = Collections.unmodifiableSortedSet(new TreeSet<>());

	/**
	 * @param group   the name of the group
	 * @param service the name of the service
	 * @return a set of nodes for the given group and service
	 */
	final SortedSet<String> getGroupService(final String group, final String service) {
		final Cache cc = cache;
		final TreeSet<String> groupSet = group == null ? null : cc.cacheGroupsMap.get(group);
		final TreeSet<String> serviceSet = service == null ? null : cc.cacheServicesMap.get(service);
		if (!StringUtils.isEmpty(group) && (groupSet == null || groupSet.isEmpty()))
			return EMPTY;
		if (!StringUtils.isEmpty(service) && (serviceSet == null || serviceSet.isEmpty()))
			return EMPTY;
		if (groupSet == null && serviceSet == null)
			return EMPTY;
		final TreeSet<String> nodes = new TreeSet<>();
		if (groupSet == null) {
			nodes.addAll(serviceSet);
			return nodes;
		}
		if (serviceSet == null) {
			nodes.addAll(groupSet);
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
		return getNodes(group, cache.cacheGroupsMap);
	}

	final TreeSet<String> getByService(final String service) {
		return getNodes(service, cache.cacheServicesMap);
	}

	final TreeMap<String, TreeSet<String>> getGroups() {
		return cache.cacheGroupsMap;
	}

	final TreeMap<String, TreeSet<String>> getServices() {
		return cache.cacheServicesMap;
	}

	/**
	 * @return a map which contains the nodes
	 */
	final Map<String, ClusterNode> getNodesMap() {
		return cache.cacheNodesMap;
	}

	final Set<SocketAddress> getExternalNodeAddresses() {
		return cache.externalNodesAddresses;
	}

	final Set<SocketAddress> getFullNodeAddresses() {
		return cache.fullNodesAddresses;
	}

	private static void registerSet(final Collection<String> keys, final HashMap<String, HashSet<String>> nodesMap,
			final String address) {
		for (String key : keys) {
			HashSet<String> nodeSet = nodesMap.get(key);
			if (nodeSet == null) {
				nodeSet = new HashSet<>();
				nodesMap.put(key, nodeSet);
				nodeSet.add(address);
			} else if (!nodeSet.contains(address))
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

	private ClusterNode put(final String httpAddress, final UUID nodeLiveId, final Long expirationTimeNs) {
		final ClusterNodeAddress clusterNodeAddress = new ClusterNodeAddress(httpAddress);
		final ClusterNode node = new ClusterNode(clusterNodeAddress, nodeLiveId, expirationTimeNs);
		nodesMap.put(clusterNodeAddress.httpAddressKey, node);
		return node;
	}

	private ClusterNode registerNode(final String httpAddress, final UUID nodeLiveId, final Long expirationTimeMs) {
		final ClusterNode node = nodesMap.get(httpAddress);
		if (node == null)
			return put(httpAddress, nodeLiveId, expirationTimeMs);
		if (nodeLiveId == null)
			return node;
		if (nodeLiveId.equals(node.nodeLiveId)) {
			node.setExpirationTime(expirationTimeMs);
			return node;
		}
		return put(httpAddress, nodeLiveId, expirationTimeMs);
	}

	/**
	 * Insert or update a node
	 *
	 * @param httpAddress the address of the node
	 */
	final ClusterNode register(final String httpAddress) {
		if (httpAddress == null)
			return null;
		return readWriteLock.write(() -> {
			final ClusterNode clusterNode = registerNode(httpAddress, null, null);
			cache = new Cache();
			return clusterNode;
		});
	}

	final ClusterNode register(final FullContent message) {
		if (message == null)
			return null;
		final String address = message.getAddress();
		if (address == null)
			return null;
		return readWriteLock.writeEx(() -> {
			final long expirationTimeMs = System.currentTimeMillis() + ProtocolListener.TWICE_DEFAULT_PERIOD_MS;
			final ClusterNode clusterNode =
					registerNode(address, message.getNodeLiveId(), expirationTimeMs);
			unregisterSet(groupsMap, clusterNode.address.httpAddressKey);
			unregisterSet(servicesMap, clusterNode.address.httpAddressKey);
			clusterNode.registerGroups(message.groups);
			clusterNode.registerServices(message.services);
			registerSet(message.groups, groupsMap, address);
			registerSet(message.services, servicesMap, address);
			cache = new Cache();
			return clusterNode;
		});
	}

	/**
	 * Unregister the node
	 *
	 * @param address the node to unregister
	 */
	private void unregisterAll(final String address) {
		if (LOGGER.isInfoEnabled())
			LOGGER.info("Unregister " + address + " from " + myAddress);
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
	final void unregister(final AddressContent message) {
		if (message == null)
			return;
		readWriteLock.writeEx(() -> {
			unregisterAll(message.getAddress());
			cache = new Cache();
		});
	}

	final synchronized void removeExpired() {
		final List<String> deleteAdresses = new ArrayList<>();
		final long currentMs = System.currentTimeMillis();
		readWriteLock.writeEx(() -> {
			nodesMap.forEach((address, node) -> {
				if (node.isExpired(currentMs))
					deleteAdresses.add(address);
			});
			if (deleteAdresses.isEmpty())
				return;
			deleteAdresses.forEach(this::unregisterAll);
			cache = new Cache();
		});
	}
}
