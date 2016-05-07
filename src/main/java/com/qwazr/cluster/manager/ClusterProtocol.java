/**
 * Copyright 2014-2016 Emmanuel Keller / QWAZR
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
import com.qwazr.utils.server.UdpServerThread;

import java.io.*;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

enum ClusterProtocol {

	join('J', Full.class),
	notify('N', Address.class),
	forward('F', Full.class),
	reply('R', Full.class),
	leave('L', Address.class);

	private final static String CHAR_HEADER = "QWAZR";

	public final char cmd;
	public final Class<? extends Externalizable> messageClass;

	ClusterProtocol(final char cmd, final Class<? extends Externalizable> messageClass) {
		this.cmd = cmd;
		this.messageClass = messageClass;
	}

	final static ClusterProtocol findCommand(final char cmd) {
		for (ClusterProtocol command : values())
			if (command.cmd == cmd)
				return command;
		throw new IllegalArgumentException("Command not found: " + cmd);
	}

	final static Message newJoin(final String address, final UUID nodeLiveId, final Set<String> groups,
			final Set<String> services) {
		return new Message(join, new Full(address, nodeLiveId, groups, services));
	}

	final static Message newNotify(final Address address) {
		return new Message(notify, address);
	}

	final static Message newForward(final String address, final UUID nodeLiveId, final Set<String> groups,
			final Set<String> services) {
		return new Message(forward, new Full(address, nodeLiveId, groups, services));
	}

	final static Message newReply(final String address, final UUID nodeLiveId, final Set<String> groups,
			final Set<String> services) {
		return new Message(reply, new Full(address, nodeLiveId, groups, services));
	}

	public static Message newLeave(final String address, final UUID nodeLiveId) {
		return new Message(leave, new Address(address, nodeLiveId));
	}

	static class Message implements Externalizable {

		private ClusterProtocol command;
		private Externalizable content;

		private Message(final ClusterProtocol command, final Externalizable content) {
			this.command = command;
			this.content = content;
		}

		Message(DatagramPacket packet) throws IOException, ReflectiveOperationException {
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
			out.writeUTF(CHAR_HEADER);
			out.writeChar(command.cmd);
			content.writeExternal(out);
		}

		@Override
		final public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
			if (!CHAR_HEADER.equals(in.readUTF()))
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
		 * Send the message using UDP (Datagram)
		 *
		 * @param recipients
		 * @param exclusions
		 * @throws IOException
		 */
		final Message send(final Collection<InetSocketAddress> recipients, final InetSocketAddress... exclusions)
				throws IOException {
			DatagramUtils.send(this, UdpServerThread.DEFAULT_BUFFER_SIZE, recipients, exclusions);
			return this;
		}

		final Message send(final String httpAddress) throws IOException, URISyntaxException {
			DatagramUtils.send(this, UdpServerThread.DEFAULT_BUFFER_SIZE, new ClusterNodeAddress(httpAddress).address);
			return this;
		}
	}

	static class Address implements Externalizable {

		/**
		 * The address of the node
		 */
		private String address;
		/**
		 * The UUID of the node
		 */
		private UUID nodeLiveId;

		Address() {
		}

		private Address(final String address, final UUID nodeLiveId) {
			this.address = address;
			this.nodeLiveId = nodeLiveId;
		}

		String getAddress() {
			return address;
		}

		UUID getNodeLiveId() {
			return nodeLiveId;
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			out.writeUTF(address);
			out.writeLong(nodeLiveId.getMostSignificantBits());
			out.writeLong(nodeLiveId.getLeastSignificantBits());
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			this.address = in.readUTF();
			this.nodeLiveId = new UUID(in.readLong(), in.readLong());
		}
	}

	static class Full extends Address {

		final Set<String> groups;
		final Set<String> services;

		Full() {
			groups = new HashSet<>();
			services = new HashSet<>();
		}

		private Full(final String address, final UUID nodeLiveId, final Set<String> groups,
				final Set<String> services) {
			super(address, nodeLiveId);
			this.groups = groups;
			this.services = services;
		}

		private static void writeCollection(final Collection<String> collection, final ObjectOutput out)
				throws IOException {
			if (collection != null) {
				out.writeInt(collection.size());
				for (String s : collection)
					out.writeUTF(s);
			} else
				out.writeInt(0);
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			super.writeExternal(out);
			writeCollection(groups, out);
			writeCollection(services, out);
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			super.readExternal(in);
			int size = in.readInt();
			while (size-- > 0)
				groups.add(in.readUTF());
			size = in.readInt();
			while (size-- > 0)
				services.add(in.readUTF());
		}
	}

}
