package io.github.hectorvent.floci.services.sqs;

/** A successful enqueue notification shared by every producer protocol. */
public record QueueActivity(String accountId, String region, String queueName) {}
