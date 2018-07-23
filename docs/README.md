QWAZR Cluster
=============

[![Build Status](https://travis-ci.org/qwazr/cluster.svg?branch=master)](https://travis-ci.org/qwazr/cluster)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.qwazr/qwazr-cluster/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.qwazr/qwazr-cluster)
[![Coverage Status](https://coveralls.io/repos/github/qwazr/cluster/badge.svg?branch=master)](https://coveralls.io/github/qwazr/cluster?branch=master)

A centralized service for distributed systems which collect and provide information
about services and group of services.

The purpose of QWAZR cluster is to provide a simple and safe way to expose a set of services to a set of clients. 

The general principle is simple:
- Any **Services** can register itself to the **Cluster** by using datagram messages.
- **Clients** request the **Cluster** to obtain the **Endpoints** of existing services.
- **Clients** can then contact the **Services** by using the provided **Endpoints**.

The QWAZR Cluster service can be used in two different ways:
- **Standalone JSON Web service :**
It exposes a JSON Web service API that let any service registering and unregistering itself.
- **Embedded in a JAVA application :**
The library manages the network connections and provide a JAVA service which let you request the cluster information.


How does it work?
-----------------

Some definitions.

### A Service instance
 
A service instance is any arbitrary service running on a server that wants to be visible on the cluster.

It is defined by:
- **A generic service name :** This name describe the kind of service provided by the instance.
- **A group list :**  the groups this service belongs.
- **A public endpoint**: The hostname name and the port this service can be contacted with by the clients.

A service will register itself to the cluster nodes.

### A Cluster master node

An instance of QWAZR Cluster which is in charge of sharing information about available services.

Several cluster master node share information.
Each instance are then synchronized and will provide the same information.

### A cluster Client

A cluster client is any program that want to get information about existing services and their location.
The client 

![Typical typology](images/cluster-standard-typology.svg)

Open source
-----------
The source code of the project is hosted at
[github/qwazr/cluster](https://github.com/qwazr/cluster).

As a QWAZR component it is released under the
[Apache 2 license](https://www.apache.org/licenses/LICENSE-2.0).