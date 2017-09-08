package com.microsoft.kafkaavailability.module;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.microsoft.kafkaavailability.IPropertiesManager;
import com.microsoft.kafkaavailability.PropertiesManager;
import com.microsoft.kafkaavailability.properties.AppProperties;
import com.microsoft.kafkaavailability.properties.MetaDataManagerProperties;
import com.microsoft.kafkaavailability.properties.ReporterProperties;
import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AppModule extends AbstractModule {
    private final static Logger LOGGER = LoggerFactory.getLogger(AppModule.class);


    private final CommandLine commandLine;
    private final String reporterPropertiesFileName;
    private final String appPropertiesFileName;
    private final String metadatamanagerPropertiesFileName;


    public AppModule(CommandLine commandLine, String reporterPropertiesFileName, String appPropertiesFileName, String metadatamanagerPropertiesFileName) {
        this.commandLine = commandLine;
        this.reporterPropertiesFileName = reporterPropertiesFileName;
        this.appPropertiesFileName = appPropertiesFileName;
        this.metadatamanagerPropertiesFileName = metadatamanagerPropertiesFileName;
    }

    @Override
    protected void configure() {
        //Use single metric registry across the board, as the class is thread safe.
        bind(MetricRegistry.class).toInstance(new MetricRegistry());

        bind(ReportersModule.class);
        bind(MonitorTasksModule.class);
    }

    @Provides
    @Singleton
    public ReporterProperties reporterProperties() {
        try {
            IPropertiesManager reporterPropertiesManager = new PropertiesManager<>(reporterPropertiesFileName, ReporterProperties.class);
            return (ReporterProperties) reporterPropertiesManager.getProperties();
        } catch (IOException e) {
            LOGGER.error("Failed to read reporter properties file at " + reporterPropertiesFileName);
            throw new RuntimeException("Failed to load reporters properties.");
        }
    }

    @Provides
    @Singleton
    public AppProperties appProperties() {
        try {
            IPropertiesManager appPropertiesManager = new PropertiesManager<>(appPropertiesFileName, AppProperties.class);
            AppProperties appProperties = (AppProperties) appPropertiesManager.getProperties();

            if(appProperties.environmentName == null || appProperties.environmentName.equals("")) {
                if(commandLine.hasOption("cluster")) {
                    appProperties.environmentName = commandLine.getOptionValue("cluster");
                } else {
                    throw new IllegalArgumentException("cluster name must be provided either on the command line or in the app properties");
                }
            }

            if(commandLine.hasOption("keyStorePassword")) {
                appProperties.keyStoreFilePassword = commandLine.getOptionValue("keyStorePassword");
            }

            return appProperties;

        } catch (IOException e) {
            LOGGER.error("Faled to load app properties file at " + appPropertiesFileName);
            throw new RuntimeException("Failed to load app properties file.");
        }
    }

    @Provides
    @Singleton
    public MetaDataManagerProperties metaDataManagerProperties() {
        try {
            IPropertiesManager metaDataPropertiesManager = new PropertiesManager<>(metadatamanagerPropertiesFileName, MetaDataManagerProperties.class);
            return (MetaDataManagerProperties) metaDataPropertiesManager.getProperties();
        } catch (IOException e) {
            LOGGER.error("Faled to load metadata manager properties file at " + metadatamanagerPropertiesFileName);
            throw new RuntimeException("Failed to load metadata manager properties file.");
        }
    }
}
