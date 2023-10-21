const { Kafka } = require("kafkajs");
const axios = require("axios");
const dotenv = require("dotenv");
dotenv.config();

const kafka = new Kafka({
  clientId: "kafka-order",
  brokers: ["localhost:9092", "localhost:9092"],
});
const consumer = kafka.consumer({
  groupId: "kafka-consumer1",
});

const pushMessage = async (message) => {
  const { userLineUid, orderId } = JSON.parse(message);
  const orderMessage = `Your order id: ${orderId} has been created`;

  try {
    const response = await axios.post(
      "https://api.line.me/v2/bot/message/push",
      {
        to: userLineUid,
        messages: [
          {
            type: "text",
            text: orderMessage,
          },
        ],
      },
      {
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${process.env.LINE_ACCESS_TOKEN}`,
        },
      }
    );

    console.log("da: response", response);
  } catch (error) {
    console.log("pushMessage err: ", error.message);
  }
};

const consume = async () => {
  await consumer.connect();
  await consumer.subscribe({
    topic: "order-message",
    fromBeginning: true,
  });

  consumer.run({
    eachMessage: async ({ partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
      });
      await pushMessage(message.value.toString());
    },
  });
};

consume().catch(console.error);
