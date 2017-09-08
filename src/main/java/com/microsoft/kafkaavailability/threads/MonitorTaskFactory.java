package com.microsoft.kafkaavailability.threads;

import java.util.List;
import java.util.concurrent.Phaser;

/**
 * This interface is used in collaboration with @Assisted and @Inject annotations for dependency injection.
 *
 * Parameters defined in creation methods are pass-in parameters, everything else are injected through Google Guice,
 * see AvailabilityThread class for constructor example, App class for usage example
 *
 */
public interface MonitorTaskFactory {
    AvailabilityThread createAvailabilityThread(Phaser phaser, long threadSleepTime);
    ProducerThread createProducerThread(Phaser phaser, long threadSleepTime);
    ConsumerThread createConsumerThread(Phaser phaser, List<String> listServers, long threadSleepTime);
    LeaderInfoThread createLeaderInfoThread(Phaser phaser, long threadSleepTime);
}