//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.discovery;
import java.util.HashMap; 
import java.util.Map; 
import java.util.UUID;

import java.util.Collection;
import java.util.Comparator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.EnsurePath; 
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance; 
import org.apache.curator.x.discovery.ServiceInstanceBuilder;
import org.apache.curator.x.discovery.ServiceProvider;
import org.apache.curator.x.discovery.ServiceType;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** * Provide static utility methods. 
 */ 
public abstract class ServiceUtil {

    private static final Logger log = LoggerFactory.getLogger(ServiceUtil.class);

    /**
     * Convert service specification into a map. Perform error checking.
     *
     * @param serviceSpec - service specification
     * @return Map of services
     * @throws IllegalArgumentException is thrown if the input cannot be properly parsed.
     */
    protected static Map<String, Integer> parseServiceSpec(String serviceSpec) {
        Map<String, Integer> serviceMap = new HashMap<String, Integer>();

        if (serviceSpec != null) {
            String[] services = serviceSpec.split(",");

            if (services.length < 1) {
                throw new IllegalArgumentException("Invalid service specification: No services found.");
            }

            for (int i = 0; i < services.length; i++) {
                String service = services[i];
                int index = service.indexOf(":");

                if (index < 0) {
                    throw new IllegalArgumentException("Invalid service specification: No name or port defined.");
                }

                Integer port = Integer.valueOf(Integer.parseInt(service.substring(index + 1)));
                serviceMap.put(service.substring(0, index), port);
            }
        }

        return serviceMap;
    }

    /**
     * Gets the single instance of RegistrationClient.
     *
     * @return single instance of RegistrationClient
     * @throws Exception the exception
     */
    protected static ServiceInstance<MetaData> configureServiceInstance(
            String serviceName,
            int servicePort,
            String serviceAddress,
            Map<String, String> parameters) throws Exception {

        ServiceInstanceBuilder<MetaData> builder = ServiceInstance.builder();

        // Address is optional.  The Curator library will automatically use the IP from the first 
        // ethernet device 
        String registerAddress = (serviceAddress == null) ? builder.build().getAddress() : serviceAddress;

        MetaData metadata = new MetaData(UUID.randomUUID(), registerAddress, servicePort, serviceName);
        metadata.setParameters(parameters);

        builder.name(serviceName).payload(metadata).id(registerAddress + ":" +
                String.valueOf(servicePort)).serviceType(ServiceType.DYNAMIC).address(registerAddress).port(servicePort);

        return builder.build();
    }

    /**
     * Gets the discovery.
     *
     * @param basePath         - Registration path
     * @param curatorFramework - Curator
     * @return the discovery
     * @throws Exception the exception
     */
    protected static ServiceDiscovery<MetaData> getServiceDiscovery(CuratorFramework curatorFramework, String basePath)
            throws Exception {

        new EnsurePath(basePath).ensure(curatorFramework.getZookeeperClient());

        JsonInstanceSerializer<MetaData> serializer = new JsonInstanceSerializer<MetaData>(MetaData.class); // Payload Serializer
        ServiceDiscovery<MetaData> serviceDiscovery = ServiceDiscoveryBuilder.builder(MetaData.class).client(curatorFramework).basePath(basePath).serializer(serializer).build(); // Service Discovery
        serviceDiscovery.start();

        return serviceDiscovery;
    }


    /**
     * Gets the discovery.
     *
     * @param basePath         - Registration path
     * @param curatorFramework - Curator
     * @return the ServiceCacheBuilder
     * @throws Exception the exception
     */
    protected static ServiceDiscovery<MetaData> getServiceCacheBuilder(CuratorFramework curatorFramework, String basePath)
            throws Exception {

        new EnsurePath(basePath).ensure(curatorFramework.getZookeeperClient());

        JsonInstanceSerializer<MetaData> serializer = new JsonInstanceSerializer<MetaData>(MetaData.class); // Payload Serializer
        ServiceDiscovery<MetaData> serviceDiscovery = ServiceDiscoveryBuilder.builder(MetaData.class).client(curatorFramework).basePath(basePath).serializer(serializer).build(); // Service Discovery
        serviceDiscovery.start();

        return serviceDiscovery;
    }


    /**
     * Gets the discovery.
     *
     * @param basePath         - Registration path
     * @param curatorFramework - Curator
     * @return the ServiceProvider
     * @throws Exception the exception
     */
    protected static ServiceProvider<MetaData> getServiceProvider(CuratorFramework curatorFramework, String basePath, String serviceName)
            throws Exception {

        new EnsurePath(basePath).ensure(curatorFramework.getZookeeperClient());

        JsonInstanceSerializer<MetaData> serializer = new JsonInstanceSerializer<MetaData>(MetaData.class); // Payload Serializer
        ServiceDiscovery<MetaData> serviceDiscovery = getServiceDiscovery(curatorFramework, basePath);
        ServiceProvider<MetaData> serviceProvider = serviceDiscovery.serviceProviderBuilder().serviceName(serviceName).build(); // Service Provider for a particular service
        serviceProvider.start();

        return serviceProvider;
    }
}