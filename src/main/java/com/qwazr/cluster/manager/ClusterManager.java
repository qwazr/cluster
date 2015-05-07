/**
 * Copyright 2015 OpenSearchServer Inc.
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

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qwazr.cluster.client.ClusterMultiClient;
import com.qwazr.cluster.client.ClusterSingleClient;
import com.qwazr.cluster.manager.ClusterNodeSet.Cache;
import com.qwazr.cluster.service.ClusterNodeRegisterJson;
import com.qwazr.cluster.service.ClusterNodeStatusJson;
import com.qwazr.cluster.service.ClusterServiceStatusJson;
import com.qwazr.cluster.service.ClusterServiceStatusJson.StatusEnum;
import com.qwazr.utils.server.ServerException;
import com.qwazr.utils.threads.PeriodicThread;

public class ClusterManager {

	private static final Logger logger = LoggerFactory
			.getLogger(ClusterManager.class);

	public static volatile ClusterManager INSTANCE = null;

	public static void load(String myAddress, File directory)
			throws IOException {
		if (INSTANCE != null)
			throw new IOException("Already loaded");
		try {
			INSTANCE = new ClusterManager(myAddress, directory);
			if (INSTANCE.isMaster()) {
				// First, we get the node list from another master (if any)
				ClusterManager.INSTANCE.loadNodesFromOtherMaster();
				// All is set, let's start the monitoring
				INSTANCE.startMonitoringThread();
			}
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
	}

	public static final String MASTER_SERVICE_NAME = "master".intern();

	public static final String CLUSTER_CONFIGURATION_NAME = "cluster.yaml";

	private final ClusterNodeMap clusterNodeMap;

	private final Set<String> clusterMasterSet;

	private final ClusterMultiClient clusterClient;

	public final String myAddress;

	private List<PeriodicThread> periodicThreads = null;

	private Thread clusterNodeShutdownThread = null;

	private final boolean isMaster;

	private ClusterManager(String publicAddress, File rootDirectory)
			throws IOException, URISyntaxException {
		myAddress = ClusterNode.toAddress(publicAddress);
		logger.info("Server: " + myAddress);

		// Load the configuration
		File clusterConfigurationFile = new File(rootDirectory,
				CLUSTER_CONFIGURATION_NAME);
		logger.info("Load configuration file: "
				+ clusterConfigurationFile.getAbsolutePath());
		ClusterConfiguration clusterConfiguration = ClusterConfiguration
				.newInstance(clusterConfigurationFile);

		// No configuration file ? Okay, we are a simple node
		if (clusterConfiguration == null
				|| clusterConfiguration.masters == null
				|| clusterConfiguration.masters.isEmpty()) {
			clusterMasterSet = null;
			clusterNodeMap = null;
			clusterClient = null;
			isMaster = false;
			logger.info("No cluster configuration. This node is not part of a cluster.");
			return;
		}

		// Build the master list and check if I am a master
		boolean isMaster = false;
		clusterMasterSet = new HashSet<String>();
		for (String master : clusterConfiguration.masters) {
			String masterAddress = ClusterNode.toAddress(master);
			logger.info("Add a master: " + masterAddress);
			clusterMasterSet.add(masterAddress);
			if (masterAddress == myAddress) {
				isMaster = true;
				logger.info("I am a master!");
			}
		}
		clusterClient = new ClusterMultiClient(clusterMasterSet, 60000);
		this.isMaster = isMaster;
		if (!isMaster) {
			clusterNodeMap = null;
			isMaster = false;
			return;
		}

		// We load the cluster node map
		clusterNodeMap = new ClusterNodeMap();
	}

	/**
	 * Load the node list from another master
	 */
	void loadNodesFromOtherMaster() {
		for (String master : clusterMasterSet) {
			if (master == myAddress)
				continue;
			try {
				logger.info("Get node list from  " + master);
				Map<String, Set<String>> nodesMap = new ClusterSingleClient(
						master, 60000).getNodes();
				if (nodesMap == null)
					continue;
				for (Map.Entry<String, Set<String>> entry : nodesMap.entrySet())
					upsertNode(entry.getKey(), entry.getValue());
				break;
			} catch (Exception e) {
				logger.warn("Unable to load the node list from " + master, e);
			}
		}
	}

	/**
	 * Start the monitoring thread
	 */
	private synchronized void startMonitoringThread() {
		if (!isMaster)
			return;
		if (periodicThreads != null)
			return;
		logger.info("Starting the periodc threads");
		periodicThreads = new ArrayList<PeriodicThread>(2);
		periodicThreads.add(new ClusterMasterThread(600));
		periodicThreads.add(new ClusterMonitoringThread(60));
	}

	private ClusterNodeMap checkMaster() throws ServerException {
		if (clusterNodeMap == null)
			throw new ServerException(Status.NOT_ACCEPTABLE,
					"I am not a master");
		return clusterNodeMap;
	}

	public ClusterNode upsertNode(String address, Set<String> services)
			throws URISyntaxException, ServerException {
		return checkMaster().upsert(address, services);
	}

	void updateNodeStatus(ClusterNode node) throws ServerException {
		checkMaster().status(node);
	}

	public ClusterNode removeNode(String address) throws URISyntaxException,
			ServerException {
		return checkMaster().remove(address);
	}

	public List<ClusterNode> getNodeList() throws ServerException {
		return checkMaster().getNodeList();
	}

	public Set<String> getMasterSet() {
		return clusterMasterSet;
	}

	public boolean isMaster() {
		return isMaster;
	}

	private List<String> buildList(ClusterNode[] nodes) {
		if (nodes == null)
			return ClusterServiceStatusJson.EMPTY_LIST;
		List<String> nodeNameList = new ArrayList<String>();
		for (ClusterNode node : nodes)
			nodeNameList.add(node.address);
		return nodeNameList;
	}

	private Cache getNodeSetCache(String service) throws ServerException {
		ClusterNodeSet nodeSet = checkMaster().getNodeSet(service);
		if (nodeSet == null)
			return null;
		return nodeSet.getCache();
	}

	public List<String> getInactiveNodes(String service) throws ServerException {
		Cache cache = getNodeSetCache(service);
		if (cache == null)
			return ClusterServiceStatusJson.EMPTY_LIST;
		return buildList(cache.activeArray);
	}

	public List<String> getActiveNodes(String service) throws ServerException {
		Cache cache = getNodeSetCache(service);
		if (cache == null)
			return ClusterServiceStatusJson.EMPTY_LIST;
		return buildList(cache.activeArray);
	}

	/**
	 * @param service
	 *            the name of the service
	 * @return a randomly choosen node
	 * @throws ServerException
	 *             if any error occurs
	 */
	public String getActiveNodeRandom(String service) throws ServerException {
		Cache cache = getNodeSetCache(service);
		if (cache == null)
			return null;
		ClusterNode[] aa = cache.activeArray;
		if (aa == null || aa.length == 0)
			return null;
		return aa[RandomUtils.nextInt(0, aa.length)].address;
	}

	/**
	 * Build a status of the given service. The list of active nodes and the
	 * list of inactive nodes with their latest status.
	 * 
	 * @param service
	 *            the name of the service
	 * @return the status of the service
	 * @throws ServerException
	 *             if any error occurs
	 */
	public ClusterServiceStatusJson getServiceStatus(String service)
			throws ServerException {
		Cache cache = getNodeSetCache(service);
		if (cache == null)
			return new ClusterServiceStatusJson();
		List<String> activeList = buildList(cache.activeArray);
		if (cache.inactiveArray == null)
			return new ClusterServiceStatusJson(activeList,
					ClusterServiceStatusJson.EMPTY_MAP);
		Map<String, ClusterNodeStatusJson> inactiveMap = new LinkedHashMap<String, ClusterNodeStatusJson>();
		for (ClusterNode node : cache.inactiveArray)
			inactiveMap.put(node.address, node.getStatus());
		return new ClusterServiceStatusJson(activeList, inactiveMap);
	}

	public void registerMe(Collection<String> services) {
		if (services == null || services.isEmpty())
			return;
		registerMe(services.toArray(new String[services.size()]));
	}

	public void registerMe(String... services) {
		if (clusterClient == null || clusterMasterSet == null
				|| services == null || services.length == 0)
			return;
		logger.info("Registering to the master");
		clusterClient
				.register(new ClusterNodeRegisterJson(myAddress, services));
		if (clusterNodeShutdownThread == null) {
			clusterNodeShutdownThread = new Thread() {
				@Override
				public void run() {
					try {
						unregisterMe();
					} catch (Exception e) {
						logger.warn(e.getMessage(), e);
					}
				}
			};
			Runtime.getRuntime().addShutdownHook(clusterNodeShutdownThread);
		}
	}

	public void unregisterMe() throws URISyntaxException {
		if (clusterClient == null)
			return;
		logger.info("Unregistering from masters");
		clusterClient.unregister(myAddress);
	}

	public TreeMap<String, StatusEnum> getServicesStatus() {
		HashMap<String, ClusterNodeSet> servicesMap = clusterNodeMap
				.getServicesMap();
		if (servicesMap == null)
			return null;
		TreeMap<String, StatusEnum> servicesStatusMap = new TreeMap<String, StatusEnum>();
		for (Map.Entry<String, ClusterNodeSet> entry : servicesMap.entrySet()) {
			Cache cache = entry.getValue().getCache();
			StatusEnum status = ClusterServiceStatusJson.findStatus(
					cache.activeArray.length, cache.inactiveArray.length);
			servicesStatusMap.put(entry.getKey(), status);
		}
		return servicesStatusMap;
	}

	public Map<String, Date> getLastExecutions() {
		if (periodicThreads == null)
			return null;
		Map<String, Date> threadsMap = new HashMap<String, Date>();
		for (PeriodicThread thread : periodicThreads)
			threadsMap.put(thread.getName(), thread.getLastExecutionDate());
		return threadsMap;
	}

	public ClusterMultiClient getClusterClient() {
		return clusterClient;
	}
}
