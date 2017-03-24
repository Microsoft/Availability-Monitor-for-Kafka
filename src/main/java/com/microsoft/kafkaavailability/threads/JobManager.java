//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.threads;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.microsoft.kafkaavailability.discovery.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class JobManager implements Callable<Long> {
    final static Logger m_logger = LoggerFactory.getLogger(JobManager.class);
    protected long timeout;
    protected TimeUnit timeUnit;
    protected Callable<Long> job;
    protected String threadName;

    public JobManager(long timeout, TimeUnit timeUnit, Callable<Long> job, String threadName) {
        this.timeout = timeout;
        this.timeUnit = timeUnit;
        this.job = job;
        this.threadName = threadName;
    }

    @Override
    public Long call() {
        Long elapsedTime = 0L;
        ExecutorService executorService = Executors.newSingleThreadExecutor(new
                ThreadFactoryBuilder().setNameFormat(threadName)
                .build());
        Future<Long> future = null;
        try {
            future = executorService.submit(job);
            elapsedTime = future.get(timeout, timeUnit);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            if (e instanceof TimeoutException) {
                m_logger.error("Thread Timeout of " + timeout + " " + timeUnit + " occurred for " + job.toString() + " Cancelling the thread:" + threadName);
            } else {
                m_logger.error("Exception occurred for " + job.toString() + " : " + e);
            }
        } finally {
            future.cancel(true);
            CommonUtils.shutdownAndAwaitTermination(executorService, job.toString());
        }
        return elapsedTime;
    }
}