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
package com.qwazr.cluster;

import com.qwazr.server.GenericServer;
import com.qwazr.server.RemoteService;
import com.qwazr.server.ServerException;
import com.qwazr.server.configuration.ServerConfiguration;
import com.qwazr.utils.ArrayUtils;
import com.qwazr.utils.HashUtils;
import com.qwazr.utils.StringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.UUID;

public class ClusterManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(ClusterManager.class);

	final ClusterNodeMap clusterNodeMap;

	final ClusterNodeAddress me;

	final ClusterNodeAddress webApp;

	final Set<String> myServices;
	final Set<String> myGroups;

	final UUID nodeLiveId;

	private final Set<String> masters;

	private final ProtocolListener protocolListener;

	private final ClusterServiceBuilder serviceBuilder;

	public ClusterManager(final GenericServer.Builder builder) throws URISyntaxException, UnknownHostException {

		final ServerConfiguration configuration = builder.getConfiguration();

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
		clusterNodeMap = new ClusterNodeMap(this, me.address);
		clusterNodeMap.register(me.httpAddressKey);
		clusterNodeMap.register(masters);

		if (configuration.multicastConnector.address != null && configuration.multicastConnector.port != -1)
			protocolListener = new MulticastListener(this, configuration.multicastConnector.address,
					configuration.multicastConnector.port);
		else
			protocolListener = new DatagramListener(this);

		builder.webService(ClusterServiceImpl.class);
		builder.packetListener(protocolListener);
		builder.startedListener(server -> {
			protocolListener.joinCluster(server.getWebServiceNames());
			protocolListener.start();
		});
		builder.shutdownListener(server -> protocolListener.leaveCluster());
		builder.contextAttribute(this);

		serviceBuilder = new ClusterServiceBuilder(new ClusterServiceImpl(this));
	}

	public ClusterServiceInterface getService() {
		return serviceBuilder.local;
	}

	public ClusterServiceBuilder getServiceBuilder() {
		return serviceBuilder;
	}

	boolean isGroup(String group) {
		if (group == null)
			return true;
		if (myGroups == null)
			return true;
		if (group.isEmpty())
			return true;
		return myGroups.contains(group);
	}

	boolean isLeader(final String service, final String group) throws ServerException {
		SortedSet<String> nodes = clusterNodeMap.getGroupService(group, service);
		if (nodes == null || nodes.isEmpty()) {
			if (LOGGER.isWarnEnabled())
				LOGGER.warn("No node available for this service/group: " + service + '/' + group);
			return false;
		}
		return me.httpAddressKey.equals(nodes.first());
	}

	final ClusterStatusJson getStatus() {
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

	final Set<String> getNodes() {
		final Map<String, ClusterNode> nodesMap = clusterNodeMap.getNodesMap();
		return nodesMap == null ? Collections.emptySet() : nodesMap.keySet();
	}

	final TreeMap<String, ClusterServiceStatusJson.StatusEnum> getServicesStatus(final String group) {
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

	final ClusterServiceStatusJson getServiceStatus(final String group, final String service) {
		final SortedSet<String> nodes = getNodesByGroupByService(group, service);
		return nodes == null || nodes.isEmpty() ? new ClusterServiceStatusJson() : new ClusterServiceStatusJson(nodes);
	}

	final SortedSet<String> getNodesByGroupByService(final String group, final String service) {
		if (StringUtils.isEmpty(group))
			return clusterNodeMap.getByService(service);
		else if (StringUtils.isEmpty(service))
			return clusterNodeMap.getByGroup(group);
		else
			return clusterNodeMap.getGroupService(group, service);
	}

	final String getLeaderNode(final String group, final String service) {
		final SortedSet<String> nodes = getNodesByGroupByService(group, service);
		if (nodes == null || nodes.isEmpty())
			return null;
		return nodes.first();
	}

	final String getRandomNode(final String group, final String service) {
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

	<T> T getService(final Collection<String> nodes, final ServiceBuilderInterface<T> builder)
			throws URISyntaxException {
		Objects.requireNonNull(builder, "No builder given");
		Objects.requireNonNull(nodes, "No nodes given");
		if (nodes.size() == 0)
			throw new NullPointerException("No nodes given");
		if (nodes.size() == 1)
			return getService(nodes.iterator().next(), builder);
		else
			return builder.remotes(RemoteService.build(nodes));
	}

	<T> T getService(final String node, final ServiceBuilderInterface<T> builder) throws URISyntaxException {
		Objects.requireNonNull(builder, "No builder given");
		Objects.requireNonNull(node, "No node given");
		return me.httpAddressKey.equals(node) ? builder.local() : builder.remote(new RemoteService(node));
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
