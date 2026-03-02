import { Kafka, type Producer, Partitioners, type Message } from "kafkajs";


/**
 * we need to keep the log immutable for every dp query 
 */
export interface AuditEvent {
  event_id: string;
  timestamp: string;
  user_id: string;
  query_hash: string;     
  query_type: string;      
  mechanism: "laplace" | "gaussian";
  epsilon_used: number;
  delta_used: number | null;
  sensitivity: number;
  noise_scale: number;
  budget_before: number;
  budget_after: number;
  result_row_count: number;
  numeric_columns: string[];
  status: "success" | "budget_exceeded" | "error";
  error?: string;
}

const TOPIC = "dp-audit-log";
let producer: Producer | null = null;
let kafka: Kafka | null = null;

export async function getKafkaProducer(): Promise<Producer> {
  if (producer) return producer;

  kafka = new Kafka({
    clientId: "dp-analytics",
    brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
    retry: {
      retries: 3,
      initialRetryTime: 300,
    },
  });

  producer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
  });

  await producer.connect();
  console.log("[Kafka] Producer connected");
  const admin = kafka.admin();
  await admin.connect();
  const topics = await admin.listTopics();
  if (!topics.includes(TOPIC)) {
    await admin.createTopics({
      topics: [{ topic: TOPIC, numPartitions: 3, replicationFactor: 1 }],
    });
    console.log(`[Kafka] Created topic: ${TOPIC}`);
  }
  await admin.disconnect();

  return producer;
}

export async function logAuditEvent(event: AuditEvent): Promise<void> {
  try {
    const prod = await getKafkaProducer();
    const message: Message = {
      key: event.user_id,         
      value: JSON.stringify(event),
      headers: {
        event_type: "dp_query_audit",
        version: "1",
      },
      timestamp: Date.now().toString(),
    };

    await prod.send({ topic: TOPIC, messages: [message] });
  } catch (err) {
    console.error("[Kafka] Failed to log audit event:", err);
    await fallbackLog(event);
  }
}

async function fallbackLog(event: AuditEvent): Promise<void> {
  const line = JSON.stringify(event) + "\n";
  await Bun.write(Bun.file("./audit-fallback.jsonl"), line, { mode: "a" as any });
}

export async function consumeRecentAuditEvents(limit = 20): Promise<AuditEvent[]> {
  if (!kafka) {
    kafka = new Kafka({
      clientId: "dp-analytics-consumer",
      brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
    });
  }

  const consumer = kafka.consumer({ groupId: `dp-audit-reader-${Date.now()}` });
  const events: AuditEvent[] = [];

  try {
    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

    await new Promise<void>((resolve) => {
      let messageCount = 0;
      consumer.run({
        eachMessage: async ({ message }) => {
          if (message.value) {
            try {
              events.push(JSON.parse(message.value.toString()) as AuditEvent);
            } catch {}
          }
          messageCount++;
          if (messageCount >= limit) resolve();
        },
      });
      setTimeout(resolve, 2000);
    });
  } finally {
    await consumer.disconnect();
  }

  return events.slice(-limit);
}

export function generateEventId(userId: string, timestamp: string): string {
  return `${userId}-${Buffer.from(timestamp).toString("base64").slice(0, 12)}`;
}

export async function hashQuery(query: string): Promise<string> {
  const encoder = new TextEncoder();
  const data = encoder.encode(query);
  const hash = await crypto.subtle.digest("SHA-256", data);
  return Array.from(new Uint8Array(hash))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("")
    .slice(0, 16);
}