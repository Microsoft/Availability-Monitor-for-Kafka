//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.metrics;

public interface IMetricsFactory {

 /**
  * Configure the component
  *
  * @throws Exception
  */
 void configure(String clusterName) throws Exception;

 /**
  * Sets the component to active (optional, may be active immediate on
  * configuration)
  *
  * @throws Exception
  */
 void start() throws Exception;

 /**
  * Stop the component / disabled (optional, may be ignored by component)
  *
  * @throws Exception
  */
 void stop() throws Exception;
}