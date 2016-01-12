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

import com.qwazr.cluster.client.ClusterMultiClient;
import com.qwazr.cluster.client.ClusterSingleClient;
import com.qwazr.cluster.manager.ClusterNodeSet.Cache;
import com.qwazr.cluster.service.*;
import com.qwazr.cluster.service.ClusterServiceStatusJson.StatusEnum;
import com.qwazr.utils.StringUtils;
import com.qwazr.utils.server.ServerException;
import com.qwazr.utils.threads.PeriodicThread;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response.Status;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ClusterManager {

	public final static String SERVICE_NAME_CLUSTER = "cluster";

	private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);

	static ClusterManager INSTANCE = null;

	public synchronized static Class<? extends ClusterServiceInterface> load(String myAddress, File dataDirectory)
					throws IOException {
		if (INSTANCE != null)
			throw new IOException("Already loaded");
		try {
			INSTANCE = new ClusterManager(myAddress);
			if (INSTANCE.isMaster()) {
				// First, we get the node list from another master (if any)
				ClusterManager.INSTANCE.loadNodesFromOtherMaster();
				// All is set, let's start the monitoring
				INSTANCE.clusterMasterThread = (ClusterMasterThread) INSTANCE
								.addPeriodicThread(new ClusterMasterThread(120));
				INSTANCE.clusterMonitoringThread = (ClusterMonitoringThread) INSTANCE
								.addPeriodicThread(new ClusterMonitoringThread(60));
			}
			return ClusterServiceImpl.class;
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
	}

	final public static ClusterManager getInstance() {
		if (INSTANCE == null)
			throw new RuntimeException("The cluster service is not enabled");
		return INSTANCE;
	}

	private final ClusterNodeMap clusterNodeMap;

	private final String[] clusterMasterArray;

	private final ClusterMultiClient clusterClient;

	public final String myAddress;

	private List<PeriodicThread> periodicThreads = null;

	private ClusterMasterThread clusterMasterThread = null;

	private ClusterMonitoringThread clusterMonitoringThread = null;

	private volatile ClusterRegisteringThread clusterRegisteringThead = null;

	private Thread clusterNodeShutdownThread = null;

	private final ConcurrentHashMap<String, Long> checkTimeMap;

	private final AtomicLong lastTimeCheck;

	private final boolean isMaster;

	private final boolean isCluster;

	private ClusterManager(String publicAddress) throws IOException, URISyntaxException {
		myAddress = ClusterNode.toAddress(publicAddress);
		logger.info("Server: " + myAddress);

		// Load the configuration
		String masters_env = System.getenv("QWAZR_MASTERS");

		// No configuration file ? Okay, we are a simple node
		if (StringUtils.isEmpty(masters_env)) {
			clusterMasterArray = null;
			clusterNodeMap = null;
			clusterClient = null;
			checkTimeMap = null;
			lastTimeCheck = null;
			isMaster = false;
			isCluster = false;
			logger.info("No QWAZR_MASTERS environment variable. This node is not part of a cluster.");
			return;
		}

		// Store the last time a master checked the node
		checkTimeMap = new ConcurrentHashMap<>();
		lastTimeCheck = new AtomicLong();

		// Build the master list and check if I am a master
		boolean isMaster = false;
		HashSet<String> masterSet = new HashSet<>();
		int i = 0;
		String[] masters = StringUtils.split(masters_env, ',');
		for (String master : masters) {
			String masterAddress = ClusterNode.toAddress(master.trim());
			logger.info("Add a master: " + masterAddress);
			masterSet.add(masterAddress);
			if (masterAddress == myAddress) {
				isMaster = true;
				logger.info("I am a master!");
			}
		}
		isCluster = true;
		clusterMasterArray = masterSet.toArray(new String[masterSet.size()]);
		clusterClient = new ClusterMultiClient(clusterMasterArray, 60000);
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
		for (String master : clusterMasterArray) {
			if (master == myAddress)
				continue;
			try {
				logger.info("Get node list from  " + master);
				Map<String, ClusterNodeJson> nodesMap = new ClusterSingleClient(master, 60000).getNodes();
				if (nodesMap == null)
					continue;
				for (ClusterNodeJson node : nodesMap.values())
					upsertNode(node);
				break;
			} catch (Exception e) {
				logger.warn("Unable to load the node list from " + master, e);
			}
		}
	}

	/**
	 * Start the periodic threads
	 */
	private synchronized PeriodicThread addPeriodicThread(PeriodicThread periodicThread) {
		logger.info("Starting the periodic thread " + periodicThread.getName());
		if (periodicThreads == null)
			periodicThreads = new ArrayList<PeriodicThread>(3);
		periodicThreads.add(periodicThread);
		return periodicThread;
	}

	private ClusterNodeMap checkMaster() throws ServerException {
		if (clusterNodeMap == null)
			throw new ServerException(Status.NOT_ACCEPTABLE, "I am not a master");
		return clusterNodeMap;
	}

	public ClusterNode upsertNode(ClusterNodeJson clusterNodeJson) throws URISyntaxException, ServerException {
		ClusterNode clusterNode = checkMaster().upsert(clusterNodeJson);
		clusterMonitoringThread.checkNode(clusterNode);
		return clusterNode;
	}

	void updateNodeStatus(ClusterNode node) throws ServerException {
		checkMaster().status(node);
	}

	public ClusterNode removeNode(String address) throws URISyntaxException, ServerException {
		return checkMaster().remove(address);
	}

	public List<ClusterNode> getNodeList() throws ServerException {
		return checkMaster().getNodeList();
	}

	public String[] getMasterArray() {
		return clusterMasterArray;
	}

	public boolean isMaster() {
		return isMaster;
	}

	public boolean isCluster() {
		return isCluster;
	}

	private static String[] buildArray(ClusterNode[]... nodesArray) {
		if (nodesArray == null)
			return ArrayUtils.EMPTY_STRING_ARRAY;
		int count = 0;
		for (ClusterNode[] nodes : nodesArray)
			if (nodes != null)
				count += nodes.length;
		if (count == 0)
			return ArrayUtils.EMPTY_STRING_ARRAY;

		String[] array = new String[count];
		int i = 0;
		for (ClusterNode[] nodes : nodesArray)
			for (ClusterNode node : nodes)
				array[i++] = node.address;
		return array;
	}

	public Cache getNodeSetCacheService(String service, String group) throws ServerException {
		ClusterNodeSet nodeSet = checkMaster().getNodeSetByService(service, group);
		if (nodeSet == null)
			return null;
		return nodeSet.getCache();
	}

	public static String[] getAllNodes(Cache cache) throws ServerException {
		if (cache == null)
			return ArrayUtils.EMPTY_STRING_ARRAY;
		return buildArray(cache.activeArray, cache.inactiveArray);
	}

	public static String[] getInactiveNodes(Cache cache) throws ServerException {
		if (cache == null)
			return ArrayUtils.EMPTY_STRING_ARRAY;
		return buildArray(cache.inactiveArray);
	}

	public static String[] getActiveNodes(Cache cache) throws ServerException {
		if (cache == null)
			return ArrayUtils.EMPTY_STRING_ARRAY;
		return buildArray(cache.activeArray);
	}

	/**
	 * @param cache the service cache
	 * @return a randomly choosen node
	 * @throws ServerException if any error occurs
	 */
	public static String getActiveNodeRandom(Cache cache) throws ServerException {
		if (cache == null)
			return null;
		final ClusterNode[] aa = cache.activeArray;
		if (aa == null || aa.length == 0)
			return null;
		return aa[RandomUtils.nextInt(0, aa.length)].address;
	}

	/**
	 * Build a status of the given service. The list of active nodes and the
	 * list of inactive nodes with their latest status.
	 *
	 * @param cache the name of the service
	 * @return the status of the service
	 * @throws ServerException if any error occurs
	 */
	public static ClusterServiceStatusJson getStatus(Cache cache) throws ServerException {
		if (cache == null)
			return new ClusterServiceStatusJson();
		String[] activeList = buildArray(cache.activeArray);
		if (cache.inactiveArray == null)
			return new ClusterServiceStatusJson(cache.master, activeList, Collections.emptyMap());
		Map<String, ClusterNodeStatusJson> inactiveMap = new LinkedHashMap<String, ClusterNodeStatusJson>();
		for (ClusterNode node : cache.inactiveArray)
			inactiveMap.put(node.address, node.getStatus());
		return new ClusterServiceStatusJson(cache.master, activeList, inactiveMap);
	}

	public synchronized void registerMe(ClusterNodeJson clusterNodeDef) {
		if (clusterClient == null || clusterMasterArray == null)
			return;
		if (clusterRegisteringThead != null) {
			logger.error("Node already registered");
			return;
		}
		clusterRegisteringThead = (ClusterRegisteringThread) addPeriodicThread(
						new ClusterRegisteringThread(90, clusterClient, clusterNodeDef));
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

	private static TreeMap<String, StatusEnum> getStatusMap(HashMap<String, ClusterNodeSet> nodeMap) {
		TreeMap<String, StatusEnum> statusMap = new TreeMap<String, StatusEnum>();
		if (nodeMap == null)
			return statusMap;
		for (Map.Entry<String, ClusterNodeSet> entry : nodeMap.entrySet()) {
			Cache cache = entry.getValue().getCache();
			StatusEnum status = ClusterServiceStatusJson
							.findStatus(cache.activeArray.length, cache.inactiveArray.length);
			statusMap.put(entry.getKey(), status);
		}
		return statusMap;
	}

	public TreeMap<String, StatusEnum> getServicesStatusMap(String group) throws ServerException {
		return getStatusMap(checkMaster().getServicesMap(group == null ? StringUtils.EMPTY : group));
	}

	public Map<String, Date> getLastExecutions() {
		if (periodicThreads == null)
			return null;
		Map<String, Date> threadsMap = new HashMap<String, Date>();
		for (PeriodicThread thread : periodicThreads)
			threadsMap.put(thread.getName(), thread.getLastExecutionDate());
		return threadsMap;
	}

	/**
	 * Called by a master when a master check the node
	 *
	 * @param masterAddress the public address of the master
	 */
	public void check(String masterAddress) {
		long time = System.currentTimeMillis();
		if (lastTimeCheck != null) {
			if (lastTimeCheck.getAndSet(time) == 0)
				if (logger.isInfoEnabled())
					logger.info("Initial check by master: " + masterAddress);
		}
		if (checkTimeMap != null && masterAddress != null)
			checkTimeMap.put(masterAddress, time);
	}

	/**
	 * @return the last time the node was checked
	 */
	public Long getLastCheck() {
		if (lastTimeCheck == null)
			return null;
		long res = lastTimeCheck.get();
		return res == 0 ? null : res;
	}

	public void removeOldCheck(long removeTime) {
		if (checkTimeMap == null)
			return;
		Iterator<Map.Entry<String, Long>> iterator = checkTimeMap.entrySet().iterator();
		while (iterator.hasNext()) {
			if (iterator.next().getValue() < removeTime)
				iterator.remove();
		}
	}

	public ClusterMultiClient getClusterClient() {
		return clusterClient;
	}

}
