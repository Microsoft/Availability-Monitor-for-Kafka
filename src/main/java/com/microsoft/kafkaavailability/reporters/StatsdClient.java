package com.microsoft.kafkaavailability.reporters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.regex.Pattern;

/**
 * A client to a StatsD server.
 */
public class StatsdClient implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(StatsdClient.class);

    private static final Pattern WHITESPACE = Pattern.compile("[\\s]+");

    public static enum StatType { COUNTER, TIMER, GAUGE }

    private final String host;
    private final int port;

    private boolean prependNewline = false;

    private ByteArrayOutputStream outputData;
    private DatagramSocket datagramSocket;
    private Writer writer;
    private final long MAX_FLUSH_BUFFER = 1400;

    public StatsdClient(String host, int port) {
        this.host = host;
        this.port = port;

        outputData = new ByteArrayOutputStream();
    }

    public void connect() throws IllegalStateException, SocketException {
        if(datagramSocket != null) {
            throw new IllegalStateException("Already connected");
        }

        prependNewline = false;

        datagramSocket = new DatagramSocket();

        outputData.reset();
        this.writer = new BufferedWriter(new OutputStreamWriter(outputData));
    }

    public void send(String name, String value, StatType statType) throws IOException {
        String statTypeStr = "";
        switch (statType) {
            case COUNTER:
                statTypeStr = "c";
                break;
            case GAUGE:
                statTypeStr = "g";
                break;
            case TIMER:
                statTypeStr = "ms";
                break;
        }

        //mdm supports max of 4k, max_flush is 4000, so there is some room for sanitizeString, formating
        int newBufferSize = outputData.size() + value.length() + name.length();

        if(newBufferSize >= MAX_FLUSH_BUFFER){
            send();
        }

        try {
            if (prependNewline) {
                writer.write("\n");
            }
            writer.write(sanitizeString(name));
            writer.write(":");
            writer.write(value);
            writer.write("|");
            writer.write(statTypeStr);
            prependNewline = true;
            writer.flush();
        } catch (IOException e) {
            logger.error("Error sending to StatsdClient:", e);
        }
    }

    @Override
    public void close() throws IOException {
        send();
        if(datagramSocket != null) {
            datagramSocket.close();
        }
        this.datagramSocket = null;
        this.writer = null;
    }

    private void send() throws IOException {
        DatagramPacket packet = newPacket(outputData);
        byte[] data = outputData.toByteArray();
        packet.setData(data);
        datagramSocket.send(packet);
        outputData.reset();
        logger.info("udp sending " + new String(data) + ", size:" + data.length);
    }

    private String sanitizeString(String s) {
        return WHITESPACE.matcher(s).replaceAll("-");
    }

    private DatagramPacket newPacket(ByteArrayOutputStream out) {
        byte[] dataBuffer;
        if (out != null) {
            dataBuffer = out.toByteArray();
        }
        else {
            dataBuffer = new byte[8192];
        }

        try {
            return new DatagramPacket(dataBuffer, dataBuffer.length, InetAddress.getByName(this.host), this.port);
        } catch (Exception e) {
            return null;
        }
    }
}