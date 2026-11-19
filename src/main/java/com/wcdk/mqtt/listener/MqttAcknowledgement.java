package com.wcdk.mqtt.listener;

/***
* 手动 ACK 确认对象接口：commit() 幂等确认消息，isAcknowledged() 查询确认状态；
* 同一条消息的多个消费方共享同一个确认对象，任一 commit 即完成确认。
* @author wcdk
***/
public interface MqttAcknowledgement {

    /***
    * 确认收到消息，可重复调用但只实际确认一次。
    * @author wcdk
    ***/
    void commit();

    /***
    * 是否已完成确认。
    * @author wcdk
    * @return 确认状态
    ***/
    boolean isAcknowledged();
}
