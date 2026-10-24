// The message consumers of the messages test: an SQS consumer on the AWS SDK
// for JavaScript v3 that long-polls one queue, and an AMQP consumer on amqplib
// with its automatic recovery. Each prints one line per event to stdout,
// `EVENT key=value ...`, which the test reads with kubectl logs.
//
// A message body is JSON, or text that holds it, as an SNS notification does:
//   marker    names the message in the log lines
//   work_ms   work before the message is acknowledged
//   after_ms  work after it is acknowledged
//   fail_in   a queue name: the SQS consumer of that queue makes the message
//             visible again at once, without deleting it
//
// Environment: MODE (sqs or amqp); for sqs, QUEUE_NAME, FLOCI_ENDPOINT and
// AWS_REGION, with the AWS credentials in the usual variables; for amqp,
// AMQP_URL and AMQP_QUEUE.

import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  GetQueueUrlCommand,
  ReceiveMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import amqp from 'amqplib';

const WAIT_TIME_SECONDS = 20;
const AMQP_HEARTBEAT_SECONDS = 10;

function log(event, fields = {}) {
  const pairs = Object.entries(fields).map(([key, value]) => `${key}=${String(value).replace(/\s+/g, '_')}`);
  console.log([event, `time=${new Date().toISOString()}`, ...pairs].join(' '));
}

function describe(error) {
  return `${error?.name ?? 'Error'}:${error?.code ?? ''}:${error?.message ?? error}`;
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function spec(body) {
  const marker = /marker\\*"\s*:\s*\\*"([A-Za-z0-9._-]+)/.exec(body)?.[1] ?? 'none';
  let parsed = {};
  try {
    parsed = JSON.parse(body);
  } catch {
    // Not JSON: a message with a marker only.
  }
  return {
    marker,
    workMs: Number(parsed.work_ms ?? 0),
    afterMs: Number(parsed.after_ms ?? 0),
    failIn: parsed.fail_in,
  };
}

async function sqsConsumer() {
  const queueName = process.env.QUEUE_NAME;
  const sqs = new SQSClient({
    endpoint: process.env.FLOCI_ENDPOINT,
    region: process.env.AWS_REGION,
    // The answer to a long poll is due within WAIT_TIME_SECONDS; a sleeping
    // pod's clock does not advance, so the limit covers only time awake.
    requestHandler: {
      connectionTimeout: 5000,
      requestTimeout: (WAIT_TIME_SECONDS + 15) * 1000,
      throwOnRequestTimeout: true,
    },
  });
  let queueUrl;
  while (!queueUrl) {
    try {
      ({ QueueUrl: queueUrl } = await sqs.send(new GetQueueUrlCommand({ QueueName: queueName })));
    } catch (error) {
      log('error', { call: 'GetQueueUrl', error: describe(error) });
      await sleep(2000);
    }
  }
  log('ready', { mode: 'sqs', queue: queueName });
  for (let poll = 1; ; poll++) {
    let messages;
    try {
      log('poll', { n: poll });
      const answer = await sqs.send(
        new ReceiveMessageCommand({
          QueueUrl: queueUrl,
          WaitTimeSeconds: WAIT_TIME_SECONDS,
          MaxNumberOfMessages: 1,
          MessageSystemAttributeNames: ['ApproximateReceiveCount'],
        }),
      );
      messages = answer.Messages ?? [];
      log('polled', { n: poll, messages: messages.length, attempts: answer.$metadata?.attempts ?? 1 });
    } catch (error) {
      log('poll-error', { n: poll, error: describe(error) });
      await sleep(1000);
      continue;
    }
    for (const message of messages) {
      await handleSqs(sqs, queueUrl, message);
    }
  }
}

async function handleSqs(sqs, queueUrl, message) {
  const { marker, workMs, afterMs, failIn } = spec(message.Body ?? '');
  const count = message.Attributes?.ApproximateReceiveCount ?? 'unknown';
  log('received', { marker, id: message.MessageId, count });
  const receipt = { QueueUrl: queueUrl, ReceiptHandle: message.ReceiptHandle };
  try {
    if (failIn === process.env.QUEUE_NAME) {
      await sqs.send(new ChangeMessageVisibilityCommand({ ...receipt, VisibilityTimeout: 0 }));
      log('failed', { marker, count });
      return;
    }
    if (workMs > 0) {
      log('working', { marker, ms: workMs });
      await sleep(workMs);
    }
    await sqs.send(new DeleteMessageCommand(receipt));
    log('deleted', { marker, count });
  } catch (error) {
    log('ack-error', { marker, error: describe(error) });
    return;
  }
  if (afterMs > 0) {
    await sleep(afterMs);
    log('after-done', { marker });
  }
}

async function amqpConsumer() {
  const queue = process.env.AMQP_QUEUE;
  let connections = 0;
  const setup = async (connection) => {
    const channel = await connection.createChannel();
    await channel.assertQueue(queue, { durable: true });
    await channel.prefetch(1);
    await channel.consume(queue, async (message) => {
      if (message === null) {
        log('cancelled', { queue });
        return;
      }
      const { marker, workMs } = spec(message.content.toString());
      log('received', { marker, redelivered: message.fields.redelivered });
      if (workMs > 0) {
        await sleep(workMs);
      }
      channel.ack(message);
      log('acked', { marker });
    });
  };
  const connection = await amqp.connect(`${process.env.AMQP_URL}?heartbeat=${AMQP_HEARTBEAT_SECONDS}`, {
    recovery: { setup, initialDelay: 500, maxDelay: 5000, waitForConnect: false },
  });
  connection.on('connect', () => {
    connections += 1;
    log('connected', { n: connections, queue });
  });
  connection.on('disconnect', (error) => log('disconnected', { error: describe(error) }));
  connection.on('connect-failed', (error) => log('connect-error', { error: describe(error) }));
  connection.on('error', (error) => log('connection-error', { error: describe(error) }));
  await connection.waitForConnect();
}

const mode = process.env.MODE;
if (mode === 'sqs') {
  await sqsConsumer();
} else if (mode === 'amqp') {
  await amqpConsumer();
} else {
  log('error', { error: `unknown MODE ${mode}` });
  process.exit(2);
}
