package ru.syntez.integration.pulsar.entities;

/**
 * Subscription names
 *
 * @author Skyhunter
 * @date 05.05.2021
 */
public enum SubscriptionNameEnum {

    SUBSCRIPTION_NAME("shared-demo"),
    SUBSCRIPTION_KEY_NAME("key-shared-demo");

    private final String code;

    SubscriptionNameEnum(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}