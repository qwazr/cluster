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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URISyntaxException;
import java.util.Collection;

class MulticastListener extends ProtocolListener {

	private static final Logger LOGGER = LoggerFactory.getLogger(MulticastListener.class);

	private final SocketAddress multicastSocketAddress;

	MulticastListener(final ClusterManager manager, final String multicastAddress, final Integer multicastPort) {
		super(manager);
		multicastSocketAddress = new InetSocketAddress(multicastAddress, multicastPort);
		if (LOGGER.isInfoEnabled())
			LOGGER.info("Start multicast listener: " + multicastSocketAddress);
	}

	@Override
	final public void acceptPacket(final DatagramPacket datagramPacket)
			throws IOException, ReflectiveOperationException, URISyntaxException {
		final MessageContent message = new MessageContent(datagramPacket);
		if (LOGGER.isTraceEnabled())
			LOGGER.trace(manager.me.httpAddressKey + " MULTICASTPACKET FROM: " + datagramPacket.getAddress() + " "
					+ message.getCommand() + " " + message.getContent());
		switch (message.getCommand()) {
			case join:
				manager.clusterNodeMap.register((FullContent) message.getContent());
				ClusterProtocol.newForward(manager.me.httpAddressKey, manager.nodeLiveId, manager.myGroups,
						manager.myServices).send(multicastSocketAddress);
				break;
			case forward:
				manager.clusterNodeMap.register((FullContent) message.getContent());
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
					.send(multicastSocketAddress);
		} catch (IOException e) {
			LOGGER.error("Unable to reach " + multicastSocketAddress, e);
		}
	}

	protected synchronized void leaveCluster() {
		try {
			ClusterProtocol.newLeave(manager.me.httpAddressKey, manager.nodeLiveId).send(multicastSocketAddress);
		} catch (IOException e) {
			LOGGER.error("Unable to reach " + multicastSocketAddress, e);
		}
	}

	@Override
	protected void runner() {
		try {
			ClusterProtocol
					.newForward(manager.me.httpAddressKey, manager.nodeLiveId, manager.myGroups, manager.myServices)
					.send(multicastSocketAddress);
		} catch (IOException e) {
			LOGGER.error("Error while running the multicast listener. The thread is stopped.", e);
		} finally {
			super.runner();
		}
	}
}
