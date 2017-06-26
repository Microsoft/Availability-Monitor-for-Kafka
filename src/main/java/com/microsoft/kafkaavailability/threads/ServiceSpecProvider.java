package com.microsoft.kafkaavailability.threads;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.microsoft.kafkaavailability.module.MonitorTasksModule;

import javax.inject.Singleton;

public class ServiceSpecProvider {

    private final String serviceSpec;

    @Inject
    @Singleton
    public ServiceSpecProvider(@Named(MonitorTasksModule.LOCAL_IP_CONSTANT_NAME) String localIPAddress,
                               @Named(MonitorTasksModule.CURATOR_PORT_CONSTANT_NAME) Integer curatorPort) {
        this.serviceSpec = localIPAddress + ":" + curatorPort;
    }

    public String getServiceSpec() {
        return serviceSpec;
    }
}
