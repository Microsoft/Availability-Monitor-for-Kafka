//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.metrics;

import java.util.Map;
import java.util.Objects;

public class MetricNameEncoded {
    public String name;
    public String tag;
    public Map<String, String> dimensions;

    MetricNameEncoded(String name, String tag, Map<String, String> dimensions) {
        this.name = name;
        this.tag = tag;
        this.dimensions = dimensions;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }

        if(! (obj instanceof MetricNameEncoded)) {
            return false;
        }

        MetricNameEncoded another = (MetricNameEncoded)obj;
        return Objects.equals(this.name, another.name) &&
                Objects.equals(this.tag, another.tag) &&
                Objects.equals(this.dimensions, another.dimensions);
    }
}
