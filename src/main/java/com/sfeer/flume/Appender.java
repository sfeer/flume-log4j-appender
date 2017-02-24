package com.sfeer.flume;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Layout;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Properties;

/**
 * 参考flume-ng-log4jappender:1.7.0
 */
public class Appender extends AppenderSkeleton {

    private String hosts;
    private long timeout;
    private RpcClient rpcClient;

    public Appender(Layout layout, String hosts) {
        this.layout = layout;
        this.hosts = hosts;
        this.timeout = RpcClientConfigurationConstants.DEFAULT_REQUEST_TIMEOUT_MILLIS;
        activateOptions();
    }

    public Appender(Layout layout, String hosts, long timeout) {
        this.layout = layout;
        this.hosts = hosts;
        this.timeout = timeout;
        activateOptions();
    }

    public synchronized void append(LoggingEvent event) throws FlumeException {
        if (this.rpcClient == null) {
            String hdrs1 = "Cannot Append to Appender! Appender either closed or not setup correctly!";
            LogLog.error(hdrs1);
            throw new FlumeException(hdrs1);
        } else {
            if (!this.rpcClient.isActive()) {
                this.reconnect();
            }

            // 头信息
            HashMap<String, String> hdrs = new HashMap<>();
            hdrs.put("flume.client.log4j.logger.name", event.getLoggerName());
            hdrs.put("flume.client.log4j.timestamp", String.valueOf(event.timeStamp));
            hdrs.put("flume.client.log4j.log.level", String.valueOf(event.getLevel().toInt()));
            hdrs.put("flume.client.log4j.message.encoding", "UTF8");
            // TODO 根据 event.getMessage() 内容，设置 hdrs 头信息，分发日志事件

            // 构建event
            Event flumeEvent = EventBuilder.withBody(this.layout.format(event), Charset.forName("UTF8"), hdrs);

            // 线程执行event发送，防止阻塞
            new Thread(new Append(this.rpcClient, flumeEvent)).start();
        }
    }

    private class Append implements Runnable {
        private RpcClient rpcClient;
        private Event flumeEvent;

        public Append(RpcClient rpcClient, Event flumeEvent) {
            this.rpcClient = rpcClient;
            this.flumeEvent = flumeEvent;
        }
        public void run() {
            try {
                this.rpcClient.append(this.flumeEvent);
            } catch (EventDeliveryException var7) {
                String msg = "Flume append() failed.";
                LogLog.error(msg);
                var7.printStackTrace();
                throw new FlumeException(msg + " Exception follows.", var7);
            }
        }
    }

    public synchronized void close() throws FlumeException {
        if (this.rpcClient != null) {
            try {
                this.rpcClient.close();
            } catch (FlumeException var5) {
                LogLog.error("Error while trying to close RpcClient.", var5);
                throw var5;
            } finally {
                this.rpcClient = null;
            }
        } else {
            String errorMsg = "Flume log4jappender already closed!";
            LogLog.error(errorMsg);
            throw new FlumeException(errorMsg);
        }
    }

    public boolean requiresLayout() {
        return true;
    }

    public void activateOptions() throws FlumeException {
        // 初始化配置文件
        Properties props = new Properties();
        if (StringUtils.isEmpty(this.hosts)) {
            throw new FlumeException("hosts must not be null");
        } else {
            String[] hostsAndPorts = this.hosts.split("\\s+");
            if (hostsAndPorts.length == 1) {
                // 单个flume-ng server
                props.setProperty("hosts", "h1");
                props.setProperty("hosts.h1", this.hosts);
                props.setProperty("connect-timeout", String.valueOf(this.timeout));
                props.setProperty("request-timeout", String.valueOf(this.timeout));
                props.setProperty("batch-size", "100");
                props.setProperty("maxIoWorkers", "3");
            } else {
                // 多个flume-ng server
                StringBuilder names = new StringBuilder();
                for (int millis = 0; millis < hostsAndPorts.length; ++millis) {
                    String hostAndPort = hostsAndPorts[millis];
                    String name = "h" + millis;
                    props.setProperty("hosts." + name, hostAndPort);
                    names.append(name).append(" ");
                }
                props.setProperty("hosts", names.toString());
                props.setProperty("client.type", RpcClientFactory.ClientType.DEFAULT_LOADBALANCE.toString());
                props.setProperty("host-selector", "round_robin");
                props.setProperty("backoff", "false");
                props.setProperty("maxBackoff", "0");
                props.setProperty("connect-timeout", String.valueOf(timeout));
                props.setProperty("request-timeout", String.valueOf(timeout));
                props.setProperty("batch-size", "100");
            }
        }

        try {
            this.rpcClient = RpcClientFactory.getInstance(props);
            if (this.layout != null) {
                this.layout.activateOptions();
            }
        } catch (FlumeException var4) {
            String errormsg = "RPC client creation failed! " + var4.getMessage();
            LogLog.error(errormsg);
            throw var4;
        }
    }

    private void reconnect() throws FlumeException {
        this.close();
        this.activateOptions();
    }
}
