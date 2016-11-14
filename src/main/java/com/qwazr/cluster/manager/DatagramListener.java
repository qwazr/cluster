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

import com.qwazr.utils.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.URISyntaxException;
import java.util.Collection;

class DatagramListener extends ProtocolListener {

	private static final Logger LOGGER = LoggerFactory.getLogger(DatagramListener.class);

	DatagramListener(final ClusterManager manager) {
		super(manager);
		if (LOGGER.isInfoEnabled())
			LOGGER.info("Start Datagram listener " + manager.me.httpAddressKey);
	}

	final void acceptJoin(final FullContent message) throws URISyntaxException, IOException {
		// Registering the node
		final ClusterNode node = registerNode(message);
		// Send immediatly a reply
		ClusterProtocol.newReply(manager.me.httpAddressKey, manager.nodeLiveId, manager.myGroups, manager.myServices)
				.send(node.address.address);
		// Notify the others
		ClusterProtocol.newNotify(message).send(manager.clusterNodeMap.getExternalNodeAddresses());
	}

	final void acceptNotify(final AddressContent message) throws URISyntaxException, IOException {
		final ClusterNode clusterNode = registerNode(message);
		// If we already know the node, we can leave
		if (clusterNode.nodeLiveId != null && message.getNodeLiveId().equals(clusterNode.nodeLiveId))
			return;
		// Otherwise we forward our configuration
		ClusterProtocol.newForward(manager.me.httpAddressKey, manager.nodeLiveId, manager.myGroups, manager.myServices)
				.send(clusterNode.address.address);
	}

	final void acceptAlive(final AddressContent message) throws URISyntaxException, IOException {
		ClusterProtocol.newNotify(message).send(manager.clusterNodeMap.getFullNodeAddresses());
		//TODO add TTL
	}

	final void acceptForward(final FullContent message) throws URISyntaxException, IOException {
		final ClusterNode node = registerNode(message);
		// Send back myself
		ClusterProtocol.newReply(manager.me.httpAddressKey, manager.nodeLiveId, manager.myGroups, manager.myServices)
				.send(node.address.address);
	}

	final void acceptReply(final FullContent message) throws URISyntaxException, IOException {
		registerNode(message);
	}

	@Override
	final public void acceptPacket(final DatagramPacket datagramPacket)
			throws IOException, ReflectiveOperationException, URISyntaxException {
		final MessageContent message = SerializationUtils.fromDefaultCompressedBytes(datagramPacket.getData());
		if (LOGGER.isTraceEnabled())
			LOGGER.trace(manager.me.httpAddressKey + " DATAGRAMPACKET FROM: " + datagramPacket.getAddress() + " "
					+ message.getCommand() + " " + message.getContent());
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
				manager.clusterNodeMap.unregister(message.getContent());
				break;
		}
	}

	protected synchronized void joinCluster(final Collection<String> services) {
		super.joinCluster(services);
		try {
			ClusterProtocol.newJoin(manager.me.httpAddressKey, manager.nodeLiveId, manager.myGroups, manager.myServices)
					.send(manager.clusterNodeMap.getFullNodeAddresses());
		} catch (IOException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	protected synchronized void leaveCluster() {
		try {
			ClusterProtocol.newLeave(manager.me.httpAddressKey, manager.nodeLiveId)
					.send(manager.clusterNodeMap.getExternalNodeAddresses());
		} catch (IOException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	@Override
	protected void runner() {
		try {
			ClusterProtocol.newAlive(manager.me.httpAddressKey, manager.nodeLiveId)
					.send(manager.clusterNodeMap.getExternalNodeAddresses());
		} catch (IOException e) {
			LOGGER.error(e.getMessage(), e);
		} finally {
			super.runner();
		}
	}
}
