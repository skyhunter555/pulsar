package ru.syntez.integration.pulsar.usecases;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Генерирует идентификаторы сообщений, на которых необходимо съэмулировать сбой
 * в заданном количестве в пределах общего количества сообщений
 */
public class GenerateErrorMessagesUsecase {

    public static Set execute(int errorCount, int messageCount) {
        Set errorDocIds = new HashSet<>();
        Random random = new Random();
        while (errorDocIds.size() < errorCount) {
            Integer docId = random.nextInt(messageCount);
            errorDocIds.add(docId);
        }
        return errorDocIds;
    }
}
