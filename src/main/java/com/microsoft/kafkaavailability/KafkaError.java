//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability;

public enum KafkaError {
    NO_ERROR,
    OFFSET_OUT_OF_RANGE,
    INVALID_MESSAGE,
    UNKNOWN_TOPIC_OR_PARTITION,
    INVALID_FETCH_SIZE,
    LEADER_NOT_AVAILABLE,
    NOT_LEADER_FOR_PARTITION,
    REQUEST_TIMED_OUT,
    BROKER_NOT_AVAILABLE,
    REPLICA_NOT_AVAILABLE,
    MESSAGE_SIZE_TOO_LARGE,
    STALE_CONTROLLER_EPOCH,
    OFFSET_METADATA_TOO_LARGE,
    OFFSETS_LOAD_IN_PROGRESS,
    CONSUMER_COORDINATOR_NOT_AVAILABLE,
    NOT_COORDINATOR_FOR_CONSUMER,
    UNKNOWN;

    public static KafkaError getError(int errorCode) {
        if (errorCode < 0 || errorCode >= UNKNOWN.ordinal()) {
            return UNKNOWN;
        } else {
            return values()[errorCode];
        }
    }
}