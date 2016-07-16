package com.qwazr.cluster.manager;

import com.qwazr.utils.concurrent.PeriodicThread;
import com.qwazr.utils.server.UdpServerThread;

import java.util.Collection;

abstract class ProtocolListener extends PeriodicThread implements UdpServerThread.PacketListener {

	protected final ClusterManager manager;

	protected ProtocolListener(final ClusterManager manager) {
		super("KeepAliveThread", 120);
		setDaemon(true);
		this.manager = manager;
	}

	protected synchronized void joinCluster(final Collection<String> services) {
		if (services != null) {
			manager.myServices.clear();
			manager.myServices.addAll(services);
		}
	}

	protected abstract void leaveCluster();
}
