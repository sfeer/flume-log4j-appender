package com.sfeer.flume;

import org.apache.log4j.*;

import java.io.IOException;


public class Slog {

    private final Logger logger;


    public Slog(Class clazz) throws IOException {
        logger = Logger.getLogger(clazz);
    }

    public static void init() throws IOException {
        Logger logger = Logger.getRootLogger();
        logger.setLevel(Level.DEBUG);

        Layout layout = new PatternLayout("%-d{yyyy-MM-dd HH:mm:ss} %-4r %-5p [%t] %37c %3x - %m%n");
        // 控制台输出
        logger.addAppender(new ConsoleAppender(layout, "System.out"));
        // 文件输出
        logger.addAppender(new DailyRollingFileAppender(layout, "e://app.log", "'.'yyyy-MM-dd"));
        // flume输出
        Logger.getLogger("org.apache.flume.api.NettyAvroRpcClient").setLevel(Level.ERROR);
        Logger.getLogger("org.apache.avro.ipc.NettyTransceiver").setLevel(Level.ERROR);
        logger.addAppender(new Appender(layout, "192.168.0.86:41414 192.168.0.83:41414 192.168.0.85:41414"));
    }

    public static void close() {
        LogManager.shutdown();
    }

    public void debug(String log) {
        logger.debug(log);
    }

    public void info(String log) {
        logger.info(log);
    }

    public static void main(String[] args) throws Exception {
        Slog.init();

        Slog log = new Slog(String.class);
        log.info("[AA22122] info:dad33255afaggge");
        log.debug("[AA21234] debug:haha333432haha");
        log.debug("[AA21234] debug:hahahsr344a2ha");

        Thread.sleep(2000);
        Slog.close();
    }
}
