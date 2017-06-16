package com.microsoft.kafkaavailability.module;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.microsoft.kafkaavailability.discovery.CommonUtils;
import com.microsoft.kafkaavailability.discovery.Constants;
import com.microsoft.kafkaavailability.discovery.CuratorClient;
import com.microsoft.kafkaavailability.discovery.CuratorManager;
import com.microsoft.kafkaavailability.properties.AppProperties;
import com.microsoft.kafkaavailability.properties.MetaDataManagerProperties;
import com.microsoft.kafkaavailability.threads.HeartBeatFactory;
import com.microsoft.kafkaavailability.threads.ServiceSpecProvider;
import com.microsoft.kafkaavailability.threads.ThreadFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadsModule extends AbstractModule {
    private final static Logger LOGGER = LoggerFactory.getLogger(ThreadsModule.class);

    @Override
    protected void configure() {
        bindConstant().annotatedWith(Names.named("localIPAddress")).to(CommonUtils.getIpAddress());
        bindConstant().annotatedWith(Names.named("curatorPort")).to((int) (65535 * Math.random()));

        install(new FactoryModuleBuilder().build(ThreadFactory.class));
        install(new FactoryModuleBuilder().build(HeartBeatFactory.class));
    }

    @Provides
    @Singleton
    public CuratorManager curatorManager(@Named("localIPAddress") String localIPAddress,
                                         ServiceSpecProvider serviceSpecProvider, final CuratorFramework curatorFramework,
                                         AppProperties appProperties) {

        LOGGER.info("Creating client, KAT in the Environment:" + appProperties.environmentName);

        final CuratorManager curatorManager = new CuratorManager(curatorFramework, Constants.DEFAULT_REGISTRATION_ROOT,
                localIPAddress, serviceSpecProvider.getServiceSpec());

        try {
            curatorManager.registerLocalService();

            Runtime.getRuntime().addShutdownHook(new Thread() {

                @Override
                public void run() {
                    LOGGER.info("Normal shutdown executing.");
                    curatorManager.unregisterService();
                    if (curatorFramework != null && (curatorFramework.getState().equals(CuratorFrameworkState.STARTED) || curatorFramework.getState().equals(CuratorFrameworkState.LATENT))) {
                        curatorFramework.close();
                    }
                }
            });
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }

        return curatorManager;
    }

    @Provides
    @Singleton
    public CuratorFramework curatorFramework(MetaDataManagerProperties metaDataManagerProperties) {
        return CuratorClient.getCuratorFramework(metaDataManagerProperties.zooKeeperHosts);
    }
}
