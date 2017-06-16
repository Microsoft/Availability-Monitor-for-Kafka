package com.microsoft.kafkaavailability.threads;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ServiceSpecProviderTest {
    static final private String LOCAL_IP = "LOCAL_IP";
    static final private Integer PORT = 1234;

    private ServiceSpecProvider provider;

    @Before
    public void setup() {
        provider = new ServiceSpecProvider(LOCAL_IP, PORT);
    }

    @Test
    public void getServiceSpec() {
        assertEquals(LOCAL_IP + ":" + PORT, provider.getServiceSpec());
    }
}