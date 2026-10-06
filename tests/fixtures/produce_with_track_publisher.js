// Publishes one message through the REAL /track Kafka publisher (alerts-service
// services/intent/kafkaProducer.js), for tests/test_intent_kafka_ordering_e2e.py.
// usage: node produce_with_track_publisher.js <alerts-service dir> <bootstrap> <topic> <key>
const path = require("path");

const [dir, bootstrap, topic, key] = process.argv.slice(2);
const { createKafkaPublisher } = require(path.join(dir, "services", "intent", "kafkaProducer"));

(async () => {
  const quiet = { info() {}, warn() {}, error() {} };
  const publisher = createKafkaPublisher({
    config: { brokers: [bootstrap], clientId: "ordering-test", sendTimeoutMs: 5000, maxInFlight: 10, connectionTimeoutMs: 3000 },
    logger: quiet,
  });
  const ack = await publisher.publish({ topic, key, value: JSON.stringify({ probe: true }) });
  await publisher.shutdown();
  console.log(JSON.stringify({ sentAt: Date.now(), ...ack }));
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
