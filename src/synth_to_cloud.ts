import { WebMidi } from "webmidi";
import { Kafka } from "kafkajs";
import { KAFKA_CONFIG, SCHEMA_REGISTRY_CONFIG} from "./config";
import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";

async function runSynthStream() {
  await WebMidi.enable();
  const schemaRegistryClient = new SchemaRegistry(SCHEMA_REGISTRY_CONFIG);
  const rawMidiMessageSchema = await schemaRegistryClient.getLatestSchemaId("raw_midi_messages-value");

  const kafka = new Kafka(KAFKA_CONFIG);
  const producer = kafka.producer();
  await producer.connect();

  WebMidi.inputs.forEach((input) => {
    input.addListener("midimessage", async (e) => {
      const [status, data1, data2] = e.message.data;

      if (status === 144) {
        console.log("Message", input.id, input.manufacturer, e);
        const key = input.id;
        const value = await schemaRegistryClient.encode(
          rawMidiMessageSchema,
          {
            SOURCE_MANUFACTURER: input.manufacturer,
            STATUS: status,
            DATA1: data1,
            DATA2: data2,
          }
        )
        producer.send({
          topic: "raw_midi_messages",
          messages: [
            { key, value },
          ],
        });
      }
    });
  });
}

runSynthStream();
