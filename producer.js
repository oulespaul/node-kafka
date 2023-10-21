const express = require("express");
const { Order, Product, sequelize } = require("./schema");
const { Kafka } = require("kafkajs");

const app = express();
const port = 8000;

app.use(express.json());

const kafka = new Kafka({
  clientId: "kafka-order",
  brokers: ["localhost:9092", "localhost:9092"],
});
const producer = kafka.producer();

app.post("/api/create-product", async (req, res) => {
  const newProduct = req.body;
  try {
    const product = await Product.create(newProduct);
    res.json({ data: product });
  } catch (error) {
    res.status(500).json({ message: error });
  }
});

app.post("/api/place-order", async (req, res) => {
  const { userId, productId } = req.body;
  try {
    const orderResult = await sequelize.transaction(async (t) => {
      const product = await Product.findOne({
        where: { id: productId },
      });
      if (product.amount <= 0) {
        return null;
      }

      await Product.decrement(
        { amount: 1 },
        {
          where: {
            id: productId,
          },
          transaction: t,
        }
      );

      return Order.create(
        {
          userLineUid: userId,
          status: "pending",
          productId,
        },
        { transaction: t }
      );
    });

    if (!orderResult) {
      return res.status(404).json({ message: "Out of order" });
    }

    await producer.send({
      topic: "order-message",
      messages: [
        {
          value: JSON.stringify({
            userLineUid: userId,
            orderId: orderResult.id,
          }),
        },
      ],
    });

    res.json({ message: "Order han been created", order: orderResult });
  } catch (error) {
    res.status(500).json({ message: error });
  }
});

app.listen(port, async () => {
  await sequelize.sync();
  await producer.connect();
  console.log(`Running on port: ${port}`);
});
