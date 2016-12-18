package com.qwazr.cluster.manager;

import com.qwazr.utils.concurrent.PeriodicThread;
import com.qwazr.server.UdpServerThread;

import java.util.Collection;

abstract class ProtocolListener extends PeriodicThread implements UdpServerThread.PacketListener {

	protected final ClusterManager manager;

	final private static int DEFAULT_PERIOD_SEC = 120;
	final private static int TWICE_DEFAULT_PERIOD_MS = DEFAULT_PERIOD_SEC * 1000 * 2;

	protected ProtocolListener(final ClusterManager manager) {
		super("KeepAliveThread", DEFAULT_PERIOD_SEC);
		setDaemon(true);
		this.manager = manager;
	}

	protected synchronized void joinCluster(final Collection<String> services) {
		if (services != null) {
			manager.myServices.clear();
			manager.myServices.addAll(services);
		}
	}

	protected ClusterNode registerNode(final AddressContent message) {
		final Long expirationTime = manager.isMe(message) ? null : System.currentTimeMillis() + TWICE_DEFAULT_PERIOD_MS;
		if (message instanceof FullContent)
			return manager.clusterNodeMap.registerFull((FullContent) message, expirationTime);
		else
			return manager.clusterNodeMap.registerAddress(message, expirationTime);
	}

	protected abstract void leaveCluster();

	@Override
	protected void runner() {
		manager.clusterNodeMap.removeExpired();
	}
}
