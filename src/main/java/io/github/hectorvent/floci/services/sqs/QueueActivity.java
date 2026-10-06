package io.github.hectorvent.floci.services.sqs;

/** A successful enqueue notification, independent of the producer's wire protocol. */
public record QueueActivity(String accountId, String region, String queueName) {}
