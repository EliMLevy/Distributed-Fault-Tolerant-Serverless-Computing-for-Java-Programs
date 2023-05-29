package edu.yu.cs.com3800;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public interface LoggingServer {

    default Logger initializeLogging(String fileNamePreface) throws IOException {
        return initializeLogging(fileNamePreface, false);
    }

    default Logger initializeLogging(String fileNamePreface, boolean disableParentHandlers) throws IOException {
        return createLogger("edu.yu.cs.com3800.LoggerServer." + fileNamePreface, fileNamePreface, disableParentHandlers);
    }

    static Logger createLogger(String loggerName, String fileNamePreface, boolean disableParentHandlers)
            throws IOException {
        Logger logger = Logger.getLogger(loggerName);
        logger.setUseParentHandlers(false);
        if (disableParentHandlers) {
        }
        String path = System.getProperty("logdir", "./logs/");
        FileHandler fileHandler = new FileHandler(path + fileNamePreface + ".log");
        fileHandler.setFormatter(new Formatter() {
            Formatter simpleFormatter = new SimpleFormatter();
            @Override
            public String format(LogRecord record) {
                return "[" + Thread.currentThread().getName() + "]" + this.simpleFormatter.format(record);
            }
        });
        logger.addHandler(fileHandler);
        return logger;
    }
}