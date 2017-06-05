package com.microsoft.kafkaavailability.threads;

public interface HeartBeatFactory {
    HeartBeat createHeartBeat(long threadSleepTime);
}
