package com.microsoft.kafkaavailability.threads;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import javax.inject.Singleton;

public class ServiceSpecProvider {

    private final String serviceSpec;

    @Inject
    @Singleton
    public ServiceSpecProvider(@Named("localIPAddress") String localIPAddress, @Named("curatorPort") Integer curatorPort) {
        this.serviceSpec = localIPAddress + ":" + curatorPort;
    }

    public String getServiceSpec() {
        return serviceSpec;
    }
}
