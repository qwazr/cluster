/**
 * Copyright 2015 Emmanuel Keller / QWAZR
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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qwazr.cluster.service.ClusterNodeStatusJson;
import com.qwazr.cluster.service.ClusterNodeStatusJson.State;
import com.qwazr.cluster.service.ClusterServiceInterface;
import com.qwazr.utils.server.ServerException;

public class ClusterNode implements FutureCallback<HttpResponse> {

	private static final Logger logger = LoggerFactory
			.getLogger(ClusterNode.class);

	public final String address;

	public Set<String> services;

	final URI baseURI;

	final URI checkURI;

	private volatile long latencyStart;

	private volatile String checkToken;

	private volatile ClusterNodeStatusJson clusterNodeStatus;

	/**
	 * Should never fail. The class will take care of the status of the cluster
	 * node.
	 * 
	 * @param hostname
	 *            The hostname of the node
	 * @param services
	 *            The set of services provided by this node
	 * @throws URISyntaxException
	 * @throws ServerException
	 */
	ClusterNode(String address, Set<String> services)
			throws URISyntaxException, ServerException {
		this.baseURI = toUri(address);
		this.address = baseURI.toString();
		this.services = services;
		checkURI = new URI(baseURI.getScheme(), null, baseURI.getHost(),
				baseURI.getPort(), "/cluster", null, null);
		latencyStart = 0;
		checkToken = null;
		setStatus(0, State.undetermined, null, null);
	}

	/**
	 * @return the status
	 */
	public ClusterNodeStatusJson getStatus() {
		return clusterNodeStatus;
	}

	private void setStatus(long time, State state, Long latency, String error) {
		this.clusterNodeStatus = new ClusterNodeStatusJson(time == 0 ? null
				: new Date(time), state, latency, error,
				clusterNodeStatus == null ? null
						: clusterNodeStatus.error_since);
		if (error != null)
			logger.warn(error);
		try {
			ClusterManager.INSTANCE.updateNodeStatus(this);
		} catch (ServerException e) {
			logger.error(e.getMessage(), e);
		}
	}

	void startCheck(CloseableHttpAsyncClient httpclient) {
		checkToken = UUID.randomUUID().toString();
		HttpHead httpHead = new HttpHead(checkURI);
		httpHead.setHeader(ClusterServiceInterface.HEADER_CHECK_NAME,
				checkToken);
		latencyStart = System.currentTimeMillis();
		httpclient.execute(httpHead, this);
	}

	@Override
	public void completed(HttpResponse response) {
		long time = System.currentTimeMillis();
		long latency = time - latencyStart;
		int responseCode = response.getStatusLine().getStatusCode();
		if (responseCode != 200) {
			setStatus(
					time,
					State.unexpected_response,
					latency,
					"Unexpected response: " + responseCode + " - "
							+ checkURI.toString());
			return;
		}
		Header header = response
				.getFirstHeader(ClusterServiceInterface.HEADER_CHECK_NAME);
		if (header == null) {
			setStatus(
					time,
					State.unexpected_response,
					latency,
					"Unexpected response: " + responseCode + " - "
							+ checkURI.toString());
			return;
		}
		if (!checkToken.equals(header.getValue())) {
			setStatus(
					time,
					State.unexpected_response,
					latency,
					"Unexpected response: " + responseCode + " - "
							+ checkURI.toString());
			return;
		}
		setStatus(time, State.online, latency, null);
	}

	@Override
	public void failed(Exception ex) {
		long time = System.currentTimeMillis();
		long latency = time - latencyStart;
		setStatus(time, State.unreachable, latency, "Cluster node failure  - "
				+ checkURI.toString());
	}

	@Override
	public void cancelled() {
		logger.warn("Cluster node cancelled " + checkURI.toString());
	}

	/**
	 * Check the latest known status of the node.
	 * 
	 * @return true if the node is online
	 */
	public boolean isActive() {
		ClusterNodeStatusJson cns = clusterNodeStatus;
		return cns != null && cns.online;
	}

	/**
	 * Update the service list
	 * 
	 * @param services
	 *            A list of service name
	 */
	public void setServices(Set<String> services) {
		this.services = services;
		logger.info("Update services for " + address);
	}

	private static URI toUri(String address) throws URISyntaxException {
		if (!address.contains("//"))
			address = "//" + address;
		URI u = new URI(address);
		return new URI(StringUtils.isEmpty(u.getScheme()) ? "http"
				: u.getScheme(), null, u.getHost(), u.getPort(), null, null,
				null);
	}

	/**
	 * Format an address which can be used in hashset or hashmap
	 * 
	 * @param hostname
	 *            the address and port
	 * @return the address usable as a key
	 * @throws URISyntaxException
	 *             thrown if the hostname format is not valid
	 */
	public static String toAddress(String hostname) throws URISyntaxException {
		return toUri(hostname).toString().intern();
	}
}
