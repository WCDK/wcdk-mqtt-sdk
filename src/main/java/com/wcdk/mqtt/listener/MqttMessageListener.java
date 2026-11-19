package com.wcdk.mqtt.listener;

import com.wcdk.mqtt.config.MqttProperties;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.Collections;
import java.util.List;

/***
* 编程式消息监听接口。subscriptions() 声明的主题会在连接与重连后自动订阅；
* 返回空列表表示不产生自动订阅，并接收客户端收到的全部消息。
* @author wcdk
***/
public interface MqttMessageListener {

    /***
    * 声明需要自动订阅的主题。
    * @author wcdk
    * @return 主题过滤器列表
    ***/
    default List<MqttProperties.Subscription> subscriptions() {
        return Collections.emptyList();
    }

    /***
    * 自动 ACK 模式下的消息处理入口。
    * @author wcdk
    * @param topic 实际主题
    * @param message 原始消息
    * @throws Exception 处理异常
    ***/
    default void onMessage(String topic, MqttMessage message) throws Exception {
    }

    /***
    * 手动 ACK 模式下的消息处理入口。实现该方法即可自行决定何时确认；
    * 未覆盖该方法时默认委托两参方法，并在处理结束（含异常）后由 SDK 兜底确认，避免在途窗口阻塞。
    * @author wcdk
    * @param topic 实际主题
    * @param message 原始消息
    * @param acknowledgement 确认对象，manual-ack 关闭时为 null
    * @throws Exception 处理异常
    ***/
    default void onMessage(String topic, MqttMessage message, MqttAcknowledgement acknowledgement) throws Exception {
        try {
            onMessage(topic, message);
        } finally {
            if (acknowledgement != null) {
                acknowledgement.commit();
            }
        }
    }
}
