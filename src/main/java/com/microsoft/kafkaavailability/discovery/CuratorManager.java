//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.discovery;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map; 
import java.util.concurrent.atomic.AtomicBoolean;
 
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Lists;

import org.apache.curator.framework.CuratorFramework; 
import org.apache.curator.framework.imps.CuratorFrameworkState; 
import org.apache.curator.x.discovery.ServiceDiscovery; 
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;
import org.apache.curator.x.discovery.strategies.RoundRobinStrategy;

import org.slf4j.Logger; 
import org.slf4j.LoggerFactory; 
 
/** * The CuratorManager  is a client for performing registration on your behalf, by providing a 
 * canonical view of services. This client helps you register, verify, and de-register services 
 * but does not provide any type of polling capabilities. If this is required, you should provide 
 * your own execution thread and periodically call {@link #verifyRegistrations()} method. 
 */ 
public final class CuratorManager {

    /**
     * The Curator framework.
     */
    private final CuratorFramework curatorFramework;

    /**
     * Registration base path.
     */
    private final String basePath;

    /**
     * The listen address.
     */
    private final String listenAddress;

    /**
     * Map of services to register.
     */
    private final Map<String, Integer> services;

    /**
     * Map of discovery objects and instances since there is a one to one correlation.
     */
    private Map<ServiceDiscovery<MetaData>, ServiceInstance<MetaData>> discoveryMap;

    private Map<String, ServiceProvider<MetaData>> serviceProviders = Maps.newHashMap();

    /**
     * Maintain client state of what was called by clients of this object.
     */
    private AtomicBoolean active = new AtomicBoolean(false);

    /**
     * The log.
     */
    private Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * Payload parameters."
     */
    private Map<String, String> parameters;

    /**
     * Instantiates a new registration client.
     *
     * @param curatorFramework the curator framework
     * @param basePath         Zookeeper registration path
     * @param listenAddress    Local IP address
     * @param serviceSpec      A list of services and corresponding ports.
     * @param parameters       A map of optional payload parameters.
     */
    public CuratorManager(CuratorFramework curatorFramework, String basePath,
                          String listenAddress,
                          String serviceSpec,
                          Map<String, String> parameters) {
        this(curatorFramework, basePath, listenAddress, serviceSpec);
        this.parameters = parameters;
    }

    /**
     * Instantiates a new registration client.
     *
     * @param curatorFramework the curator framework
     * @param basePath         Zookeeper registration path
     * @param listenAddress    Local IP address
     * @param serviceSpec      A list of services and corresponding ports.
     */
    public CuratorManager(CuratorFramework curatorFramework, String basePath,
                          String listenAddress,
                          String serviceSpec) {
        this(curatorFramework, basePath, listenAddress, ServiceUtil.parseServiceSpec(serviceSpec));
    }


    /**
     * Instantiates a new registration client.
     *
     * @param curatorFramework the curator framework
     * @param basePath         Zookeeper registration path
     * @param listenAddress    Local IP address
     * @param services         A Map of services and corresponding ports.
     */
    public CuratorManager(CuratorFramework curatorFramework, String basePath, String listenAddress,
                          Map<String, Integer> services) {
        this.curatorFramework = curatorFramework;
        this.basePath = basePath;
        this.listenAddress = listenAddress;
        this.services = services;
        discoveryMap = new HashMap<ServiceDiscovery<MetaData>, ServiceInstance<MetaData>>();
    }

    /**
     * Advertise availability.
     *
     * @return the registration client
     */
    public CuratorManager registerLocalService() throws Exception {

        if (active.getAndSet(true)) {
            throw new IllegalStateException("This client instance is already available.");
        }

        try {
            if (curatorFramework.getState() != CuratorFrameworkState.STARTED) {
                curatorFramework.start();
            }

            for (Map.Entry<String, Integer> entry : services.entrySet()) {
                String serviceName = entry.getKey();
                int port = entry.getValue().intValue();

                ServiceDiscovery<MetaData> serviceDiscovery = ServiceUtil.getServiceDiscovery(curatorFramework, basePath);
                ServiceInstance<MetaData> service = ServiceUtil.configureServiceInstance(serviceName, port, listenAddress, parameters);
 
                /*                 * Having >1 instance with of the same name with same listenAddress + listPort is 
                 * bad. Incur some overhead to look for duplicates and explode appropriately 
                 */
                Collection<ServiceInstance<MetaData>> candidates = serviceDiscovery.queryForInstances(serviceName);

                for (ServiceInstance<MetaData> worker : candidates) {
                    outputInstance(worker);
                    if ((worker.getAddress().equals(service.getAddress())) && (worker.getPort() == port)) {
                        log.error("An instance of " + service + " already exists at: " +
                                service.getAddress() + ":" + port);
                        throw new IllegalStateException("Duplicate service being registered. for service: " +
                                serviceName + " at: " + basePath);
                    }
                }

                log.debug("registering service: " + serviceName);
                serviceDiscovery.registerService(service);
                discoveryMap.put(serviceDiscovery, service);
                log.info("registered service: " + serviceName);
            }
        } catch (RuntimeException rte) {
            throw rte;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }

        return this;
    }

    /**
     * Verify services are registered.
     *
     * @throws Exception
     */
    public void verifyRegistrations() throws Exception {
        ServiceDiscovery<MetaData> serviceDiscovery = ServiceUtil.getServiceDiscovery(curatorFramework, basePath);
        listInstances(serviceDiscovery);

        for (Map.Entry<ServiceDiscovery<MetaData>, ServiceInstance<MetaData>> entry : discoveryMap.entrySet()) {
            ServiceInstance<MetaData> instance = entry.getValue();
            try {
                ServiceInstance<MetaData> found = entry.getKey().queryForInstance(instance.getName(), instance.getId());

                if (found == null) {
                    throw new RuntimeException("There is no instance for: " + instance.getName() + ":" + instance.getId() + " registered ");
                }

                log.debug(found.getName() + " is verified at: " + found.getAddress() + ":" + found.getPort());
            } catch (Exception e) {
                log.error("Could not find service: " + (instance.getName() + ":" + instance.getId()), e);
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * unregister Availability.
     *
     * @return the registration client
     */
    public CuratorManager unregisterService() {

        active.set(false);

        for (Map.Entry<ServiceDiscovery<MetaData>, ServiceInstance<MetaData>> entry : discoveryMap.entrySet()) {
            ServiceInstance<MetaData> instance = entry.getValue();
            String serviceName = instance.getName();
            try {
                log.debug("unregistering service: " + serviceName);
                entry.getKey().unregisterService(instance);
                log.info("service unregistered: " + serviceName);
            } catch (Exception e) {
                log.error("Unregistration exception: ", e);
            } finally {
                try {
                    entry.getKey().close();
                } catch (IOException ignore) {
                }
            }
        }

        return this;
    }

    public ServiceInstance<MetaData> getInstanceByName(ServiceDiscovery<MetaData> serviceDiscovery, String serviceName) throws Exception {
        ServiceProvider<MetaData> serviceProvider = serviceProviders.get(serviceName);

        if (serviceProvider == null) {
            serviceProvider = serviceDiscovery.serviceProviderBuilder().
                    serviceName(serviceName).
                    providerStrategy(new RoundRobinStrategy<MetaData>())
                    .build();
            serviceProvider.start();
            serviceProviders.put(serviceName, serviceProvider);

        }
        return serviceProvider.getInstance();
    }

    public Collection<ServiceInstance<MetaData>> getWorkers(String serviceName) throws Exception {
        Collection<ServiceInstance<MetaData>> instances;
        ServiceDiscovery<MetaData> serviceDiscovery = ServiceUtil.getServiceDiscovery(curatorFramework, basePath);
        try {
            instances = serviceDiscovery.queryForInstances(serviceName);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw Throwables.propagate(e);
        }

        return instances;
    }

    public void getWorkerNames(String serviceName) throws Exception {
        Collection<ServiceInstance<MetaData>> instances = getWorkers(serviceName);
        List<ServiceInstance<MetaData>> instanceList = Lists.newArrayList(ServiceUtil.getServiceDiscovery(curatorFramework, basePath).queryForInstances(serviceName));
    }

    public void listInstances(ServiceDiscovery<MetaData> serviceDiscovery) throws Exception {
        // This shows how to query all the instances in service discovery
        try {
            Collection<String> serviceNames = serviceDiscovery.queryForNames();
            for (String serviceName : serviceNames) {
                Collection<ServiceInstance<MetaData>> instances = serviceDiscovery.queryForInstances(serviceName);
                log.info("serviceName: " + serviceName);
                for (ServiceInstance<MetaData> instance : instances) {
                    outputInstance(instance);
                }
            }
        } catch (Exception e) {
                /*                 * Something bad did happen, but carry on
                 */
            log.error(e.getMessage(), e);
        }
    }

    public List<String> listServiceInstance() throws Exception {
        List<String> list = new ArrayList<String>();
        ServiceDiscovery<MetaData> serviceDiscovery = ServiceUtil.getServiceDiscovery(curatorFramework, basePath);

        try {
            Collection<String> serviceNames = serviceDiscovery.queryForNames();
            for (String serviceName : serviceNames) {
                Collection<ServiceInstance<MetaData>> instances = serviceDiscovery.queryForInstances(serviceName);
                log.info("serviceName: " + serviceName);
                for (ServiceInstance<MetaData> instance : instances) {
                    list.add(instance.getPayload().getListenAddress() + ":" + instance.getPayload().getListenPort());
                    outputInstance(instance);
                }
            }

        } catch (Exception e) {
                /*                 * Something bad did happen, but carry on
                 */
            log.error(e.getMessage(), e);
        }
        Collections.sort(list);
        log.debug("listsize:" + list.size());
        return list;
    }

    public void outputInstance(ServiceInstance<MetaData> instance) {
        log.info("\t" + instance.getPayload().toString() + ": " + instance.buildUriSpec());
    }
}