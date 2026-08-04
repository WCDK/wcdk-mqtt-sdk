package com.wcdk.mqtt.core;

import com.wcdk.mqtt.config.MqttProperties;
import com.wcdk.mqtt.listener.MqttAcknowledgement;
import com.wcdk.mqtt.listener.MqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.lang.NonNull;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @auther WCDK
 * @date 2026/8/4
 * @version 1.0
 **/
public class MqttListenerRegistrar implements BeanPostProcessor, MqttMessageListener {

    private static final Logger log = LoggerFactory.getLogger(MqttListenerRegistrar.class);

    private final List<MethodHandler> handlers = new CopyOnWriteArrayList<>();

    @Override
    public Object postProcessAfterInitialization(Object bean, @NonNull String beanName) throws BeansException {
        if (bean == this) {
            return bean;
        }
        Class<?> targetClass = ClassUtils.getUserClass(bean);
        for (Method method : getCandidateMethods(targetClass)) {
            MqttListener listener = AnnotationUtils.findAnnotation(method, MqttListener.class);
            if (listener != null) {
                registerListener(bean, beanName, method, listener);
            }
        }
        return bean;
    }

    @Override
    public List<MqttProperties.Subscription> subscriptions() {
        if (handlers.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, MqttProperties.Subscription> subscriptions = new LinkedHashMap<>();
        for (MethodHandler handler : handlers) {
            for (String topic : handler.topics) {
                MqttProperties.Subscription subscription = subscriptions.computeIfAbsent(topic, key -> {
                    MqttProperties.Subscription value = new MqttProperties.Subscription();
                    value.setTopic(key);
                    return value;
                });
                subscription.setQos(Math.max(subscription.getQos(), handler.qos));
            }
        }
        return new ArrayList<>(subscriptions.values());
    }

    @Override
    public void onMessage(String topic, MqttMessage message) throws Exception {
        dispatch(topic, message, null);
    }

    @Override
    public void onMessage(String topic, MqttMessage message, MqttAcknowledgement acknowledgement) throws Exception {
        dispatch(topic, message, acknowledgement);
    }

    private List<Method> getCandidateMethods(Class<?> targetClass) {
        List<Method> methods = new ArrayList<>();
        Class<?> currentClass = targetClass;
        while (currentClass != null && !Object.class.equals(currentClass)) {
            for (Method method : currentClass.getDeclaredMethods()) {
                if (!method.isBridge() && !method.isSynthetic()) {
                    methods.add(method);
                }
            }
            currentClass = currentClass.getSuperclass();
        }
        return methods;
    }

    private void registerListener(Object bean, String beanName, Method method, MqttListener listener) {
        List<String> topics = resolveTopics(listener);
        if (topics.isEmpty()) {
            log.error("MQTT listener method {} topic must not be empty", method.getName());
            return;
        }
        if (!validateMethod(method)) {
            return;
        }
        ReflectionUtils.makeAccessible(method);
        MethodHandler handler = new MethodHandler(bean, method, topics, normalizeQos(listener.qos()));
        handlers.add(handler);
        log.info("MQTT listener registered, beanName={}, method={}, topics={}, qos={}",
                beanName, method.getName(), topics, handler.qos);
    }

    private boolean validateMethod(Method method) {
        if (!void.class.equals(method.getReturnType())) {
            log.error("MQTT listener method {} return type must be void", method.getName());
            return false;
        }
        for (Parameter parameter : method.getParameters()) {
            Class<?> parameterType = parameter.getType();
            if (String.class == parameterType
                    || byte[].class == parameterType
                    || MqttMessage.class.isAssignableFrom(parameterType)
                    || MqttAcknowledgement.class.isAssignableFrom(parameterType)) {
                continue;
            }
            log.error("MQTT listener method {} has unsupported parameter type {}",
                    method.getName(), parameterType.getName());
            return false;
        }
        return true;
    }

    private void dispatch(String topic, MqttMessage message, MqttAcknowledgement acknowledgement) throws Exception {
        for (MethodHandler handler : handlers) {
            if (handler.matches(topic)) {
                invokeListenerMethod(handler, topic, message, acknowledgement);
            }
        }
    }

    private void invokeListenerMethod(MethodHandler handler,
                                      String topic,
                                      MqttMessage message,
                                      MqttAcknowledgement acknowledgement) throws Exception {
        try {
            log.info("MQTT listener received message, topic={}, qos={}, retained={}",
                    topic, message == null ? null : message.getQos(), message != null && message.isRetained());
            handler.method.invoke(handler.bean, resolveArguments(handler.method, topic, message, acknowledgement));
        } catch (InvocationTargetException ex) {
            Throwable targetException = ex.getTargetException();
            if (targetException instanceof Exception exception) {
                throw exception;
            }
            throw new IllegalStateException("Invoke MQTT listener method failed, method=" + handler.method.getName(), targetException);
        }
    }

    private Object[] resolveArguments(Method method,
                                      String topic,
                                      MqttMessage message,
                                      MqttAcknowledgement acknowledgement) {
        Parameter[] parameters = method.getParameters();
        Object[] args = new Object[parameters.length];
        byte[] payloadBytes = message == null || message.getPayload() == null ? new byte[0] : message.getPayload();
        String payload = new String(payloadBytes, StandardCharsets.UTF_8);
        int stringIndex = 0;
        boolean multipleStringParameters = hasMultipleStringParameters(parameters);
        for (int i = 0; i < parameters.length; i++) {
            Class<?> parameterType = parameters[i].getType();
            if (MqttMessage.class.isAssignableFrom(parameterType)) {
                args[i] = message;
            } else if (MqttAcknowledgement.class.isAssignableFrom(parameterType)) {
                args[i] = acknowledgement;
            } else if (byte[].class == parameterType) {
                args[i] = payloadBytes;
            } else if (String.class == parameterType) {
                args[i] = stringIndex++ == 0 && multipleStringParameters ? topic : payload;
            }
        }
        return args;
    }

    private boolean hasMultipleStringParameters(Parameter[] parameters) {
        int count = 0;
        for (Parameter parameter : parameters) {
            if (String.class == parameter.getType()) {
                count++;
            }
        }
        return count > 1;
    }

    private List<String> resolveTopics(MqttListener listener) {
        List<String> topics = new ArrayList<>();
        addTopics(topics, listener.value());
        addTopics(topics, listener.topics());
        return topics;
    }

    private void addTopics(List<String> topics, String[] values) {
        if (values == null) {
            return;
        }
        Arrays.stream(values)
                .filter(StringUtils::hasText)
                .map(String::trim)
                .filter(topic -> !topics.contains(topic))
                .forEach(topics::add);
    }

    private int normalizeQos(int qos) {
        if (qos < 0 || qos > 2) {
            throw new IllegalArgumentException("MQTT QoS must be 0, 1 or 2");
        }
        return qos;
    }

    private static final class MethodHandler {

        private final Object bean;

        private final Method method;

        private final List<String> topics;

        private final int qos;

        private MethodHandler(Object bean, Method method, List<String> topics, int qos) {
            this.bean = bean;
            this.method = method;
            this.topics = topics;
            this.qos = qos;
        }

        private boolean matches(String topic) {
            for (String topicFilter : topics) {
                if (matchesTopicFilter(topicFilter, topic)) {
                    return true;
                }
            }
            return false;
        }

        private boolean matchesTopicFilter(String filter, String topic) {
            if (filter.equals(topic)) {
                return true;
            }
            String[] filterLevels = filter.split("/", -1);
            String[] topicLevels = topic == null ? new String[0] : topic.split("/", -1);
            int i = 0;
            for (; i < filterLevels.length; i++) {
                String filterLevel = filterLevels[i];
                if ("#".equals(filterLevel)) {
                    return i == filterLevels.length - 1;
                }
                if (i >= topicLevels.length) {
                    return false;
                }
                if (!"+".equals(filterLevel) && !filterLevel.equals(topicLevels[i])) {
                    return false;
                }
            }
            return i == topicLevels.length;
        }
    }
}
