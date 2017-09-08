package com.microsoft.kafkaavailability;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

public class KeyStoreLoader {

    final static Logger LOGGER = LoggerFactory.getLogger(KeyStoreLoader.class);


    public static KeyStore loadKeyStore(String keyStorePath, String keyStorePassword) {
        KeyStore trustStore = null;

        try (InputStream ksStream = new FileInputStream(keyStorePath)) {

            trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStore.load(ksStream, keyStorePassword.toCharArray());
        } catch (KeyStoreException e) {
            //This should never happen since we're using default type.
            LOGGER.error("Failed to get an instance of KeyStore.");
        } catch (IOException | CertificateException | NoSuchAlgorithmException e) {
            LOGGER.error("Failed to load keyStore file at " + keyStorePath, e);
            throw new RuntimeException(e);
        }

        return trustStore;
    }
}
