/**
 * Copyright 2015-2016 Emmanuel Keller / QWAZR
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

import com.datastax.driver.core.utils.UUIDs;
import com.qwazr.cluster.service.ClusterNodeJson;
import com.qwazr.cluster.service.ClusterServiceImpl;
import com.qwazr.cluster.service.ClusterServiceStatusJson;
import com.qwazr.cluster.service.ClusterStatusJson;
import com.qwazr.utils.ArrayUtils;
import com.qwazr.utils.StringUtils;
import com.qwazr.utils.concurrent.PeriodicThread;
import com.qwazr.utils.server.ServerBuilder;
import com.qwazr.utils.server.ServerException;
import com.qwazr.utils.server.UdpServerThread;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.URISyntaxException;
import java.util.*;

public class ClusterManager implements UdpServerThread.PacketListener {

	public final static String SERVICE_NAME_CLUSTER = "cluster";

	private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);

	public static ClusterManager INSTANCE = null;

	public synchronized static void load(final ServerBuilder builder, final Collection<String> masters,
			final Collection<String> myGroups) {
		if (INSTANCE != null)
			throw new RuntimeException("Already loaded");
		try {
			INSTANCE = new ClusterManager(builder, masters, myGroups);
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
	}

	private ClusterNodeMap clusterNodeMap;

	public final ClusterNodeAddress me;
	public final ClusterNodeAddress webApp;

	private final Set<String> myServices;

	private final Set<String> myGroups;

	private final UUID nodeLiveId;

	private final KeepAliveThread keepAliveThread;

	private ClusterManager(final ServerBuilder builder, final Collection<String> masters,
			final Collection<String> myGroups) throws URISyntaxException {

		this.nodeLiveId = UUIDs.timeBased();

		String address = builder.getServerConfiguration().webServicePublicAddress;
		me = new ClusterNodeAddress(address);
		address = builder.getServerConfiguration().webApplicationPublicAddress;
		webApp = new ClusterNodeAddress(address);

		if (logger.isInfoEnabled())
			logger.info("Server: " + me.httpAddressKey + " Groups: " + ArrayUtils.prettyPrint(myGroups));
		this.myGroups = myGroups != null ? new HashSet<>(myGroups) : null;
		this.myServices = new HashSet<>(); // Will be filled later using server hook

		clusterNodeMap = new ClusterNodeMap(me.address);
		clusterNodeMap.register(me.httpAddressKey);

		if (masters != null)
			for (String master : masters)
				clusterNodeMap.register(master);

		keepAliveThread = new KeepAliveThread();

		builder.registerWebService(ClusterServiceImpl.class);
		builder.registerPacketListener(this);
		builder.registerStartedListener(server -> {
			joinCluster(server.getServiceNames());
			keepAliveThread.start();
		});
		builder.registerShutdownListener(server -> leaveCluster());
	}

	public boolean isGroup(String group) {
		if (group == null)
			return true;
		if (myGroups == null)
			return true;
		if (group.isEmpty())
			return true;
		return myGroups.contains(group);
	}

	public boolean isLeader(final String group, final String service) throws ServerException {
		SortedSet<String> nodes = clusterNodeMap.getGroupService(group, service);
		if (nodes == null || nodes.isEmpty()) {
			logger.warn("No node available for this service/group: " + service + '/' + group);
			return false;
		}
		return me.httpAddressKey.equals(nodes.first());
	}

	final public ClusterStatusJson getStatus() {
		final Map<String, ClusterNode> nodesMap = clusterNodeMap.getNodesMap();
		final TreeMap<String, ClusterNodeJson> nodesJsonMap = new TreeMap<>();
		if (nodesMap != null)
			nodesMap.forEach((address, clusterNode) -> nodesJsonMap.put(address,
					new ClusterNodeJson(clusterNode.address.httpAddressKey, clusterNode.nodeLiveId, clusterNode.groups,
							clusterNode.services)));
		return new ClusterStatusJson(me.httpAddressKey, myServices.contains("webapps") ? webApp.httpAddressKey : null,
				nodesJsonMap, clusterNodeMap.getGroups(), clusterNodeMap.getServices(),
				keepAliveThread.getLastExecutionDate());
	}

	final public Set<String> getNodes() {
		final Map<String, ClusterNode> nodesMap = clusterNodeMap.getNodesMap();
		return nodesMap == null ? Collections.emptySet() : nodesMap.keySet();
	}

	final public TreeMap<String, ClusterServiceStatusJson.StatusEnum> getServicesStatus(final String group) {
		final TreeMap<String, ClusterServiceStatusJson.StatusEnum> servicesStatus = new TreeMap();
		final Set<String> services = clusterNodeMap.getServices().keySet();
		if (services == null || services.isEmpty())
			return servicesStatus;
		services.forEach(service -> {
			final SortedSet<String> nodes = getNodesByGroupByService(group, service);
			if (nodes != null && !nodes.isEmpty())
				servicesStatus.put(service, ClusterServiceStatusJson.findStatus(nodes.size()));
		});
		return servicesStatus;
	}

	final public ClusterServiceStatusJson getServiceStatus(final String group, final String service) {
		final SortedSet<String> nodes = getNodesByGroupByService(group, service);
		return nodes == null || nodes.isEmpty() ? new ClusterServiceStatusJson() : new ClusterServiceStatusJson(nodes);
	}

	final public SortedSet<String> getNodesByGroupByService(final String group, final String service) {
		if (StringUtils.isEmpty(group))
			return clusterNodeMap.getByService(service);
		else if (StringUtils.isEmpty(service))
			return clusterNodeMap.getByGroup(group);
		else
			return clusterNodeMap.getGroupService(group, service);
	}

	final public String getLeaderNode(final String group, final String service) {
		final SortedSet<String> nodes = getNodesByGroupByService(group, service);
		if (nodes == null || nodes.isEmpty())
			return null;
		return nodes.first();
	}

	final public String getRandomNode(final String group, final String service) {
		final SortedSet<String> nodes = getNodesByGroupByService(group, service);
		if (nodes == null || nodes.isEmpty())
			return null;
		int rand = RandomUtils.nextInt(0, nodes.size());
		Iterator<String> it = nodes.iterator();
		for (; ; ) {
			final String node = it.next();
			if (rand == 0)
				return node;
			rand--;
		}
	}

	public synchronized void joinCluster(final Collection<String> services) {
		if (services != null) {
			myServices.clear();
			myServices.addAll(services);
		}
		try {
			ClusterProtocol.newJoin(me.httpAddressKey, nodeLiveId, myGroups, myServices)
					.send(clusterNodeMap.getFullNodeAddresses());
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
	}

	public synchronized void leaveCluster() {
		try {
			ClusterProtocol.newLeave(me.httpAddressKey, nodeLiveId).send(clusterNodeMap.getExternalNodeAddresses());
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
	}

	final public void acceptJoin(ClusterProtocol.Full message) throws URISyntaxException, IOException {
		// Registering the node
		final ClusterNode node = clusterNodeMap.register(message);
		// Send immediatly a reply
		ClusterProtocol.newReply(me.httpAddressKey, nodeLiveId, myGroups, myServices).send(node.address.address);
		// Notify the others
		ClusterProtocol.newNotify(message).send(clusterNodeMap.getExternalNodeAddresses());
	}

	final public void acceptNotify(ClusterProtocol.Address message) throws URISyntaxException, IOException {
		final ClusterNode clusterNode = clusterNodeMap.register(message.getAddress());
		// If we already know the node, we can leave
		if (clusterNode.nodeLiveId != null && message.getNodeLiveId().equals(clusterNode.nodeLiveId))
			return;
		// Otherwise we forward our configuration
		ClusterProtocol.newForward(me.httpAddressKey, nodeLiveId, myGroups, myServices)
				.send(clusterNode.address.address);
	}

	final public void acceptAlive(ClusterProtocol.Address message) throws URISyntaxException, IOException {
		ClusterProtocol.newNotify(message).send(clusterNodeMap.getFullNodeAddresses());
	}

	final public void acceptForward(ClusterProtocol.Full message) throws URISyntaxException, IOException {
		ClusterNode node = clusterNodeMap.register(message);
		// Send back myself
		ClusterProtocol.newReply(me.httpAddressKey, nodeLiveId, myGroups, myServices).send(node.address.address);
	}

	final public void acceptReply(ClusterProtocol.Full message) throws URISyntaxException, IOException {
		clusterNodeMap.register(message);
	}

	@Override
	final public void acceptPacket(final DatagramPacket datagramPacket)
			throws IOException, ReflectiveOperationException, URISyntaxException {
		ClusterProtocol.Message message = new ClusterProtocol.Message(datagramPacket);
		logger.info("DATAGRAMPACKET FROM: " + datagramPacket.getAddress().toString() + " " + message.getCommand());
		switch (message.getCommand()) {
		case join:
			acceptJoin(message.getContent());
			break;
		case notify:
			acceptNotify(message.getContent());
			break;
		case forward:
			acceptForward(message.getContent());
			break;
		case reply:
			acceptReply(message.getContent());
			break;
		case alive:
			acceptAlive(message.getContent());
			break;
		case leave:
			clusterNodeMap.unregister(message.getContent());
			break;
		}
	}

	private class KeepAliveThread extends PeriodicThread {

		private KeepAliveThread() {
			super("KeepAliveThread", 120);
			setDaemon(true);
		}

		@Override
		protected void runner() {
			try {
				ClusterProtocol.newAlive(me.httpAddressKey, nodeLiveId).send(clusterNodeMap.getExternalNodeAddresses());
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		}

	}

}
