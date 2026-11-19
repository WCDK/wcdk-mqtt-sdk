package com.wcdk.mqtt.core;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/***
* MQTT 监听方法注解：标注在 Spring Bean 方法上，SDK 自动按主题订阅并在重连后恢复订阅。
* @author wcdk
***/
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MqttListener {

    /***
    * 主题过滤器数组，支持 + 与 # 通配符。
    * @author wcdk
    * @return 主题过滤器数组
    ***/
    String[] topics() default {};

    /***
    * topics() 的别名。
    * @author wcdk
    * @return 主题过滤器数组
    ***/
    String[] value() default {};

    /***
    * 订阅 QoS，取值 0、1、2。
    * @author wcdk
    * @return QoS
    ***/
    int qos() default 1;
}
