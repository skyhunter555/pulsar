package ru.syntez.integration.pulsar.exceptions;

/**
 * Wrapper over RuntimeException. Includes additional options for formatting message text.
 *
 * @author Skyhunter
 * @date 26.02.2021
 */
public class TestMessageException extends RuntimeException {

    public TestMessageException(String message) {
        super(message);
    }

    public TestMessageException(String messageFormat, Object... messageArgs) {
        super(String.format(messageFormat, messageArgs));
    }

    public TestMessageException(Throwable throwable) {
        super(throwable);
    }

}
