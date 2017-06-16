package com.microsoft.kafkaavailability.threads;

import java.util.List;
import java.util.concurrent.Phaser;

public interface ThreadFactory {
    AvailabilityThread createAvailabilityThread(Phaser phaser, long threadSleepTime);
    ProducerThread createProducerThread(Phaser phaser, long threadSleepTime);
    HeartBeatThread createHeartBeatThread(String serverName);
    ConsumerThread createConsumerThread(Phaser phaser, List<String> listServers, long threadSleepTime);
    LeaderInfoThread createLeaderInfoThread(Phaser phaser, long threadSleepTime);
}