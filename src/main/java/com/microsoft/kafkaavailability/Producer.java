//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability;

import com.microsoft.kafkaavailability.properties.ProducerProperties;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Properties;

/***
 * Responsible for sending canary messages to specified topics and partitions in Kafka
 */
public class Producer implements IProducer {
    private IPropertiesManager<ProducerProperties> m_propManager;
    final static Logger m_logger = LoggerFactory.getLogger(Producer.class);
    private int m_vipRetries = 3;
    private IMetaDataManager m_metaDataManager;
    private ProducerProperties producerProperties;
    private kafka.javaapi.producer.Producer<String, String> m_producer;
    private static SSLSocketFactory m_sslSocketFactory = null;

    /***
     * @param propManager     Used to get properties from json file
     * @param metaDataManager Used to get the broker list
     */
    public Producer(IPropertiesManager<ProducerProperties> propManager, IMetaDataManager metaDataManager) throws MetaDataManagerException {
        m_metaDataManager = metaDataManager;
        m_propManager = propManager;
        producerProperties = m_propManager.getProperties();
        Properties props = new Properties();
        String brokerList = "";
        for (String broker : m_metaDataManager.getBrokerList(true)) {
            brokerList += broker + ", ";
        }
        props.put("metadata.broker.list", brokerList);
        props.put("serializer.class", producerProperties.serializer_class);
        props.put("partitioner.class", SimplePartitioner.class.getName());
        props.put("request.required.acks", producerProperties.request_required_acks.toString());

        ProducerConfig config = new ProducerConfig(props);
        m_producer = new kafka.javaapi.producer.Producer<String, String>(config);
    }

    /***
     * Sends the message to specified topic and partition
     *
     * @param topicName   topic name
     * @param partitionId partition id
     */
    @Override
    public void SendCanaryToTopicPartition(String topicName, String partitionId) {
        m_producer.send(createCanaryMessage(topicName, partitionId));
    }

    /***
     * Constructs the canary message to be sent.
     * The message is encoded with the topic and partition information to tell Kafka where it should land.
     *
     * @param topicName   topic name
     * @param partitionId partition id
     * @return
     */
    private KeyedMessage<String, String> createCanaryMessage(String topicName, String partitionId) {
        long runtime = new Date().getTime();
        String msg = producerProperties.messageStart + runtime + ",www.example.com," + partitionId;
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topicName, partitionId, msg);
        return data;
    }

    /***
     * Sends canary message to specified topic through kafkaClusterIP
     *
     * @param kafkaIP         kafkaClusterIP
     * @param topicName       topic name
     * @param enableCertCheck enable ssl certificate check. Not required if the tool trusts the kafka server
     * @throws Exception
     */

    public void SendCanaryToKafkaIP(String kafkaIP, String topicName, boolean enableCertCheck) throws Exception {
        URL obj = new URL(kafkaIP + topicName);
        HttpsURLConnection con = (HttpsURLConnection) obj.openConnection();
        if (!enableCertCheck) {
            setAcceptAllVerifier(con);
        }

        for (int i = 0; i < m_vipRetries; i++) {
            try {

                //add request header
                con.setRequestMethod("POST");
                con.setConnectTimeout(15000);
                con.setReadTimeout(15000);
                con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
                con.setRequestProperty("Content-Type", "application/octet-stream");
                con.setUseCaches(false);
                String urlParameters = producerProperties.messageStart + new Date().getTime() + ",www.example.com,";
                m_logger.debug("Sending 'POST' request to URL : " + kafkaIP + topicName);
                m_logger.debug("Post parameters : " + urlParameters);


                // Send post request
                con.setDoOutput(true);
                DataOutputStream wr = new DataOutputStream(con.getOutputStream());
                wr.writeBytes(urlParameters);
                wr.flush();
                wr.close();

                int responseCode = con.getResponseCode();

                m_logger.debug("Response Code : " + responseCode);
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(con.getInputStream()));
                String inputLine;
                StringBuffer response = new StringBuffer();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();
                //print result
                m_logger.debug(response.toString());
                break;
            } catch (Exception e) {
                m_logger.error(e.getMessage(), e);
                e.printStackTrace();
                //look for m_vipRetries - 1, otherwise you will never throw an exception in case of failures.
                if (i == m_vipRetries - 1)
                    throw e;

                try {
                    Thread.sleep(500);
                } catch (Exception ex) {
                    m_logger.error(ex.getMessage(), ex);
                }
            } finally {
                if (con != null) {
                    con.disconnect();
                }
            }
        }
    }

    protected static void setAcceptAllVerifier(HttpsURLConnection connection) throws NoSuchAlgorithmException, KeyManagementException {

        // Create the socket factory.
        // Reusing the same socket factory allows sockets to be
        // reused, supporting persistent connections.
        if (null == m_sslSocketFactory) {
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, ALL_TRUSTING_TRUST_MANAGER, new java.security.SecureRandom());
            m_sslSocketFactory = sc.getSocketFactory();
        }

        connection.setSSLSocketFactory(m_sslSocketFactory);

        // Since we may be using a cert with a different name, we need to ignore
        // the hostname as well.
        connection.setHostnameVerifier(ALL_TRUSTING_HOSTNAME_VERIFIER);
    }

    private static final TrustManager[] ALL_TRUSTING_TRUST_MANAGER = new TrustManager[]{
            new X509TrustManager() {
                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }

                public void checkClientTrusted(X509Certificate[] certs, String authType) {
                }

                public void checkServerTrusted(X509Certificate[] certs, String authType) {
                }
            }
    };

    private static final HostnameVerifier ALL_TRUSTING_HOSTNAME_VERIFIER = new HostnameVerifier() {
        public boolean verify(String hostname, SSLSession session) {
            return true;
        }
    };

    /**
     * Closes this context
     *
     * @throws IOException
     */
    public void close() throws IOException {
        if (m_producer != null) {
            m_producer.close();
        }
    }
}
