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

import com.qwazr.utils.DatagramUtils;
import com.qwazr.utils.StringUtils;
import com.qwazr.utils.server.UdpServerThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.DatagramPacket;
import java.net.SocketAddress;
import java.util.Collection;

class MessageContent implements Externalizable {

	private static final Logger LOGGER = LoggerFactory.getLogger(MessageContent.class);

	private ClusterProtocol command;
	private Externalizable content;

	MessageContent(final ClusterProtocol command, final Externalizable content) {
		this.command = command;
		this.content = content;
	}

	MessageContent(final DatagramPacket packet) throws IOException, ReflectiveOperationException {
		try (final ByteArrayInputStream bis = new ByteArrayInputStream(packet.getData())) {
			try (final ObjectInputStream ois = new ObjectInputStream(bis)) {
				readExternal(ois);
			}
		}
	}

	final ClusterProtocol getCommand() {
		return command;
	}

	final <T extends Externalizable> T getContent() {
		return (T) content;
	}

	@Override
	final public void writeExternal(final ObjectOutput out) throws IOException {
		out.writeUTF(ClusterProtocol.CHAR_HEADER);
		out.writeChar(command.cmd);
		content.writeExternal(out);
	}

	@Override
	final public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
		if (!ClusterProtocol.CHAR_HEADER.equals(in.readUTF()))
			throw new IOException("Unknown UDP message (wrong header)");
		command = ClusterProtocol.findCommand(in.readChar());
		try {
			content = command.messageClass.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new IOException(e);
		}
		content.readExternal(in);
	}

	/**
	 * Send a message using UDP (Datagram) to a collection of recipients
	 *
	 * @param recipients
	 * @throws IOException
	 */
	final MessageContent send(final Collection<SocketAddress> recipients) throws IOException {
		if (LOGGER.isTraceEnabled())
			LOGGER.trace("Send " + command.name() + " to " + StringUtils.join(recipients, ","));
		DatagramUtils.send(this, UdpServerThread.DEFAULT_BUFFER_SIZE, recipients);
		return this;
	}

	/**
	 * Send the message using UDP (Datagram)
	 *
	 * @param socketAddress
	 * @return
	 * @throws IOException
	 */
	final MessageContent send(final SocketAddress socketAddress) throws IOException {
		if (LOGGER.isTraceEnabled())
			LOGGER.trace("Send " + command.name() + " to " + socketAddress);
		DatagramUtils.send(this, UdpServerThread.DEFAULT_BUFFER_SIZE, socketAddress);
		return this;
	}

}
