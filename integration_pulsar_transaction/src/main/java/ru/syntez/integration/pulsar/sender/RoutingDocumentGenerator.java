package ru.syntez.integration.pulsar.sender;

import ru.syntez.integration.pulsar.entities.RoutingDocument;

@FunctionalInterface
public interface RoutingDocumentGenerator {
    RoutingDocument create(int id);
}
