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

import com.qwazr.cluster.service.ClusterNodeJson;
import com.qwazr.cluster.service.ClusterServiceImpl;
import com.qwazr.cluster.service.ClusterServiceStatusJson;
import com.qwazr.cluster.service.ClusterStatusJson;
import com.qwazr.utils.ArrayUtils;
import com.qwazr.utils.HashUtils;
import com.qwazr.utils.StringUtils;
import com.qwazr.server.ServerBuilder;
import com.qwazr.server.configuration.ServerConfiguration;
import com.qwazr.server.ServerException;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.*;

public class ClusterManager {

	public final static String SERVICE_NAME_CLUSTER = "cluster";

	private static final Logger LOGGER = LoggerFactory.getLogger(ClusterManager.class);

	public static ClusterManager INSTANCE = null;

	public synchronized static void load(final ServerBuilder builder, final ServerConfiguration configuration) {
		if (INSTANCE != null)
			throw new RuntimeException("Already loaded");
		try {
			INSTANCE = new ClusterManager(builder, configuration);
		} catch (URISyntaxException | UnknownHostException e) {
			throw new RuntimeException(e);
		}
	}

	ClusterNodeMap clusterNodeMap;

	final ClusterNodeAddress me;

	final ClusterNodeAddress webApp;

	final Set<String> myServices;
	final Set<String> myGroups;

	final UUID nodeLiveId;

	private final Set<String> masters;

	private final ProtocolListener protocolListener;

	private ClusterManager(final ServerBuilder builder, final ServerConfiguration configuration)
			throws URISyntaxException, UnknownHostException {

		this.nodeLiveId = HashUtils.newTimeBasedUUID();

		me = new ClusterNodeAddress(configuration.webServiceConnector.addressPort,
				configuration.webServiceConnector.port);
		webApp = new ClusterNodeAddress(configuration.webAppConnector.addressPort, configuration.webAppConnector.port);

		if (LOGGER.isInfoEnabled())
			LOGGER.info("Server: " + me.httpAddressKey + " Groups: " + ArrayUtils.prettyPrint(configuration.groups));
		this.myGroups = configuration.groups != null ? new HashSet<>(configuration.groups) : null;
		this.myServices = new HashSet<>(); // Will be filled later using server hook
		if (configuration.masters != null && !configuration.masters.isEmpty()) {
			this.masters = new HashSet<>();
			configuration.masters.forEach(master -> this.masters.add(
					new ClusterNodeAddress(master, configuration.webServiceConnector.port).httpAddressKey));
		} else
			this.masters = null;
		clusterNodeMap = new ClusterNodeMap(me.address);
		clusterNodeMap.register(me.httpAddressKey);
		clusterNodeMap.register(masters);

		if (configuration.multicastConnector.address != null && configuration.multicastConnector.port != -1)
			protocolListener = new MulticastListener(this, configuration.multicastConnector.address,
					configuration.multicastConnector.port);
		else
			protocolListener = new DatagramListener(this);

		builder.registerWebService(ClusterServiceImpl.class);
		builder.registerPacketListener(protocolListener);
		builder.registerStartedListener(server -> {
			protocolListener.joinCluster(server.getWebServiceNames());
			protocolListener.start();
		});
		builder.registerShutdownListener(server -> protocolListener.leaveCluster());
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
			if (LOGGER.isWarnEnabled())
				LOGGER.warn("No node available for this service/group: " + service + '/' + group);
			return false;
		}
		return me.httpAddressKey.equals(nodes.first());
	}

	final public ClusterStatusJson getStatus() {
		final Map<String, ClusterNode> nodesMap = clusterNodeMap.getNodesMap();
		final TreeMap<String, ClusterNodeJson> nodesJsonMap = new TreeMap<>();
		if (nodesMap != null) {
			final long currentMs = System.currentTimeMillis();
			nodesMap.forEach((address, clusterNode) -> {
				final Integer timeToLive;
				final Long expirationTimeMs = clusterNode.getExpirationTimeMs();
				if (expirationTimeMs != null)
					timeToLive = (int) ((expirationTimeMs - currentMs) / 1000);
				else
					timeToLive = null;
				final ClusterNodeJson clusterNodeJson =
						new ClusterNodeJson(clusterNode.address.httpAddressKey, clusterNode.nodeLiveId, timeToLive,
								clusterNode.groups, clusterNode.services);
				nodesJsonMap.put(address, clusterNodeJson);
			});
		}
		return new ClusterStatusJson(me.httpAddressKey, myServices.contains("webapps") ? webApp.httpAddressKey : null,
				nodesJsonMap, clusterNodeMap.getGroups(), clusterNodeMap.getServices(), masters,
				protocolListener.getLastExecutionDate());
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

	public final String getHttpAddressKey() {
		return me.httpAddressKey;
	}

	final boolean isMe(final AddressContent message) {
		if (message == null)
			return false;
		if (nodeLiveId.equals(message.getNodeLiveId()))
			return true;
		if (me.httpAddressKey.equals(message.getAddress()))
			return true;
		return false;
	}

	final boolean isMaster(final ClusterNodeAddress nodeAddress) {
		if (nodeAddress == null || masters == null)
			return false;
		return masters.contains(nodeAddress.httpAddressKey);
	}
}
