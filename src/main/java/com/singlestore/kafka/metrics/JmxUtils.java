package com.singlestore.kafka.metrics;

import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

public class JmxUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(JmxUtils.class);

    // Total 1 minute attempting to retry metrics registration in case of errors
    private static final int REGISTRATION_RETRIES = 12;
    private static final Duration REGISTRATION_RETRY_DELAY = Duration.ofSeconds(5);

    public static synchronized void registerMXBean(ObjectName objectName, Object mxBean) {
        try {
            final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            if (mBeanServer == null) {
                LOGGER.info("JMX not supported, bean '{}' not registered", objectName);
                return;
            }
            // During connector restarts it is possible that Kafka Connect does not manage
            // the lifecycle perfectly. In that case it is possible the old metric MBean is still present.
            // There will be multiple attempts executed to register new MBean.
            for (int attempt = 1; attempt <= REGISTRATION_RETRIES; attempt++) {
                try {
                    mBeanServer.registerMBean(mxBean, objectName);
                    break;
                }
                catch (InstanceAlreadyExistsException e) {
                    if (attempt < REGISTRATION_RETRIES) {
                        LOGGER.warn(
                                "Unable to register metrics as an old set with the same name: '{}' exists, retrying in {} (attempt {} out of {})",
                                objectName, REGISTRATION_RETRY_DELAY, attempt, REGISTRATION_RETRIES);
                        Thread.sleep(REGISTRATION_RETRY_DELAY.toMillis());
                    }
                    else {
                        LOGGER.error("Failed to register metrics MBean, metrics will not be available");
                    }
                }
            }
        }
        catch (JMException | InterruptedException e) {
            throw new RuntimeException("Unable to register the MBean '" + objectName + "'", e);
        }
    }

    public static synchronized void unregisterMXBean(ObjectName objectName) {
        try {
            final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            if (mBeanServer == null) {
                LOGGER.debug("JMX not supported, bean '{}' not registered", objectName);
                return;
            }
            try {
                mBeanServer.unregisterMBean(objectName);
            }
            catch (InstanceNotFoundException e) {
                LOGGER.info("Unable to unregister metrics MBean '{}' as it was not found", objectName);
            }
        }
        catch (JMException e) {
            throw new RuntimeException("Unable to unregister the MBean '" + objectName + "'", e);
        }
    }
}

