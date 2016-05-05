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

import com.qwazr.cluster.service.*;
import com.qwazr.utils.AnnotationsUtils;
import com.qwazr.utils.ArrayUtils;
import com.qwazr.utils.StringUtils;
import com.qwazr.utils.server.ServerException;
import com.qwazr.utils.server.ServiceInterface;
import com.qwazr.utils.server.ServiceName;
import com.qwazr.utils.server.UdpServerThread;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public class ClusterManager implements Consumer<DatagramPacket> {

	public final static String SERVICE_NAME_CLUSTER = "cluster";

	private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);

	public static ClusterManager INSTANCE = null;

	public synchronized static Class<? extends ClusterServiceInterface> load(final ExecutorService executor,
			final UdpServerThread udpServer, final String myAddress, final Collection<String> myGroups)
			throws IOException {
		if (INSTANCE != null)
			throw new IOException("Already loaded");
		try {
			INSTANCE = new ClusterManager(executor, udpServer, myAddress, myGroups);
			Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
					try {
						INSTANCE.unregisterMe();
					} catch (IOException e) {
						logger.warn(e.getMessage(), e);
					}
				}
			});
			return ClusterServiceImpl.class;
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
	}

	private ClusterNodeMap clusterNodeMap;

	public final String myAddress;

	private final Set<String> myGroups;

	public final ExecutorService executor;

	private final UdpServerThread udpServer;

	private ClusterManager(final ExecutorService executor, final UdpServerThread udpServer, final String publicAddress,
			final Collection<String> myGroups) throws IOException, URISyntaxException {

		this.udpServer = udpServer;

		this.executor = executor;
		myAddress = toAddress(publicAddress);

		if (logger.isInfoEnabled())
			logger.info("Server: " + myAddress + " Groups: " + ArrayUtils.prettyPrint(myGroups));
		this.myGroups = myGroups != null ? new HashSet<>(myGroups) : null;

		// Load the configuration
		String nodes_env = System.getenv("QWAZR_NODES");
		if (nodes_env == null)
			nodes_env = System.getenv("QWAZR_MASTERS");

		clusterNodeMap = new ClusterNodeMap();

		if (nodes_env != null) {
			final String[] nodes = StringUtils.split(nodes_env, ',');
			for (String node : nodes) {
				String nodeAddress = toAddress(node.trim());
				logger.info("Add a node: " + nodeAddress);
				clusterNodeMap.register(nodeAddress);
			}
		}

		if (this.udpServer != null)
			this.udpServer.register(this);

		// We load the cluster node map
		clusterNodeMap = new ClusterNodeMap();
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

	public Map<String, ClusterNodeJson> getNodesMap() throws ServerException {
		return clusterNodeMap.getNodesMap();
	}

	public boolean isLeader(String service, String group) throws ServerException {
		TreeSet<String> nodes = clusterNodeMap.getGroupService(service, group);
		if (nodes == null || nodes.isEmpty())
			return false;
		return myAddress.equals(nodes.first());
	}

	public boolean isMe(String address) {
		return myAddress.equals(address);
	}

	final public ClusterStatusJson getStatus() {
		return new ClusterStatusJson(clusterNodeMap.getNodesMap(), clusterNodeMap.getGroups(),
				clusterNodeMap.getServices());
	}

	final public TreeMap<String, ClusterServiceStatusJson.StatusEnum> getServicesStatus(final String group) {
		final TreeMap<String, ClusterServiceStatusJson.StatusEnum> servicesStatus = new TreeMap();
		final Set<String> services = clusterNodeMap.getServices().keySet();
		if (services == null || services.isEmpty())
			return servicesStatus;
		services.forEach(service -> {
			final TreeSet<String> nodes = getNodesByGroupByService(group, service);
			if (nodes != null && !nodes.isEmpty())
				servicesStatus.put(service, ClusterServiceStatusJson.findStatus(nodes.size()));
		});
		return servicesStatus;
	}

	final public ClusterServiceStatusJson getServiceStatus(final String group, final String service) {
		final TreeSet<String> nodes = getNodesByGroupByService(group, service);
		return nodes == null || nodes.isEmpty() ? new ClusterServiceStatusJson() : new ClusterServiceStatusJson(nodes);
	}

	final public TreeSet<String> getNodesByGroupByService(final String group, final String service) {
		if (StringUtils.isEmpty(group))
			return clusterNodeMap.getByService(service);
		else if (StringUtils.isEmpty(service))
			return clusterNodeMap.getByGroup(group);
		else
			return clusterNodeMap.getGroupService(group, service);
	}

	final public String getLeaderNode(final String group, final String service) {
		final TreeSet<String> nodes = getNodesByGroupByService(group, service);
		if (nodes == null || nodes.isEmpty())
			return null;
		return nodes.first();
	}

	final public String getRandomNode(final String group, final String service) {
		final TreeSet<String> nodes = getNodesByGroupByService(group, service);
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

	public synchronized void registerMe(Collection<Class<? extends ServiceInterface>> services) throws IOException {
		if (udpServer == null)
			return;
		udpServer.send(new ClusterMessage(ClusterProtocol.joinCluster, new ClusterProtocol.AddressResponse(myAddress)));
		services.forEach(service -> {
			ServiceName serviceName = AnnotationsUtils.getFirstAnnotation(service, ServiceName.class);
			Objects.requireNonNull(serviceName, "The ServiceName annotation is missing for " + service);
			try {
				udpServer.send(new ClusterMessage(ClusterProtocol.registerService,
						new ClusterProtocol.NameResponse(myAddress, serviceName.value())));
			} catch (IOException e) {
				logger.error("Failed in register the service: " + serviceName.value(), e);
			}
		});
		if (myGroups != null) {
			myGroups.forEach(group -> {
				try {
					udpServer.send(new ClusterMessage(ClusterProtocol.registerGroup,
							new ClusterProtocol.NameResponse(myAddress, group)));
				} catch (IOException e) {
					logger.error("Failed in register the group: " + group, e);
				}
			});
		}
	}

	public void unregisterMe() throws IOException {
		if (udpServer == null)
			return;
		udpServer
				.send(new ClusterMessage(ClusterProtocol.leaveCluster, new ClusterProtocol.AddressResponse(myAddress)));
	}

	@Override
	final public void accept(final DatagramPacket datagramPacket) {
		try {
			ClusterMessage message = new ClusterMessage(datagramPacket.getData());
			logger.info("DATAGRAMPACKET FROM: " + datagramPacket.getAddress().toString() + " " + message.getCommand());
			switch (message.getCommand()) {
			case joinCluster:
				clusterNodeMap.register((ClusterProtocol.AddressResponse) message.getContent());
				break;
			case leaveCluster:
				clusterNodeMap.unregister(message.getContent());
				break;
			case registerGroup:
				clusterNodeMap.registerGroup(message.getContent());
				break;
			case registerService:
				clusterNodeMap.registerService(message.getContent());
				break;
			case serviceGroupRequest:
				break;
			default:
				break;
			}
		} catch (Exception e) {
			logger.warn(e.getMessage(), e);
		}
	}

	private static URI toUri(String address) throws URISyntaxException {
		if (!address.contains("//"))
			address = "//" + address;
		URI u = new URI(address);
		return new URI(StringUtils.isEmpty(u.getScheme()) ? "http" : u.getScheme(), null, u.getHost(), u.getPort(),
				null, null, null);
	}

	/**
	 * Format an address which can be used in hashset or hashmap
	 *
	 * @param hostname the address and port
	 * @return the address usable as a key
	 * @throws URISyntaxException thrown if the hostname format is not valid
	 */
	static String toAddress(String hostname) throws URISyntaxException {
		return toUri(hostname).toString().intern();
	}

}
