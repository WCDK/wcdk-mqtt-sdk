package com.wcdk.mqtt.core;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @auther WCDK
 * @date 2026/7/8
 * @version 1.0
 **/
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MqttListener {

    /**
     * MQTT topic filters to subscribe.
     *
     * @return topic filter array
     */
    String[] topics() default {};

    /**
     * Alias for {@link #topics()}.
     *
     * @return topic filter array
     */
    String[] value() default {};

    /**
     * MQTT QoS, valid values are 0, 1 and 2.
     *
     * @return qos
     */
    int qos() default 1;
}
