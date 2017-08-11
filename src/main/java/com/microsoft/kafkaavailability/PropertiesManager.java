//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/***
 * Gets property values from json files.
 * @param <T> T is the type used to serialize and deserialize the json file.
 */
public class PropertiesManager<T> implements IPropertiesManager<T>
{
    private String m_propFileName;
    private T m_prop;
    final Class<T> m_typeParameterClass;
    final static Logger m_logger = LoggerFactory.getLogger(PropertiesManager.class);

    private static final String STRING_TYPE = "java.lang.String";
    private static final String LIST_TYPE = "java.util.List";
    private static final String INT_TYPE = "int";

    /***
     *
     * @param propFileName json file containing properties
     * @param typeParameterClass The class object associated with the type T
     * @throws IOException if property file is not found in classpath
     */
    public PropertiesManager(String propFileName, Class<T> typeParameterClass) throws IOException
    {
        this.m_propFileName = propFileName;
        m_typeParameterClass = typeParameterClass;
        Gson gson = new Gson();
        URL url = Thread.currentThread().getContextClassLoader().getResource(propFileName);

        if (url != null)
        {
            String text = Resources.toString(url, Charsets.UTF_8);
            m_prop = gson.fromJson(text, m_typeParameterClass);
            MergePropsFromEnv(m_prop);
        } else
        {
            throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
        }
    }

    /***
     *
     * @return An object of the type T that contains the properties from the json file.
     */
    public T getProperties()
    {
        return m_prop;
    }

    private void MergePropsFromEnv(Object prop){
        m_logger.debug("Overriding from en variables");
        Field[] propFields = prop.getClass().getFields();
        for(Field field : propFields){
            String envVarName = field.getName().toUpperCase();
            String override= System.getenv(envVarName);
            if(override != null){
                setProperty(field.getName(), override);
            }
        }
    }

    public void setProperty(String propName,String override){
        try {
            Field field = m_prop.getClass().getDeclaredField(propName);
            String dataType = field.getType().getCanonicalName();
            m_logger.debug("Setting " + propName + " from envirnment variable as " + override);
            if(dataType == LIST_TYPE){
                List<String> value = new ArrayList<String>(Arrays.asList(override.split(",")));
                set(field,value);
            }
            if(dataType == INT_TYPE){
                int value = Integer.parseInt(override);
                set(field,value);
            }
        }catch(NoSuchFieldException Ex){
            m_logger.error("Field cannot be found in the config "+ Ex.getMessage() );
        }
    }

    private void set(Field field,Object value){
        try{
            field.set(m_prop,value);
        }catch(IllegalAccessException Ex){
            m_logger.error("Error while setting property "+ field.getName() + Ex.getMessage());
        }
    }
}