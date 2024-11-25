const amqp = require('amqplib');

// Hàm nhận dữ liệu từ Direct Exchange
const receiveFromDirectExchange = async (queueName, bindingKey) => {
  try {
    const connection = await amqp.connect('amqp://localhost'); // Kết nối RabbitMQ
    const channel = await connection.createChannel(); // Tạo kênh giao tiếp

    const exchangeName = 'sensor_direct_exchange'; // Tên Exchange
    await channel.assertExchange(exchangeName, 'direct', { durable: true }); // Đảm bảo Exchange tồn tại

    await channel.assertQueue(queueName, { durable: true }); // Tạo hàng đợi riêng cho consumer
    await channel.bindQueue(queueName, exchangeName, bindingKey); // Liên kết hàng đợi với routing key

    console.log(`[*] Waiting for messages in queue: ${queueName} with binding key: ${bindingKey}`);

    // Nhận dữ liệu từ hàng đợi
    channel.consume(
      queueName,
      (msg) => {
        if (msg !== null) {
          const messageContent = msg.content.toString();
          const data = JSON.parse(messageContent);
          console.log(`[x] Received from queue ${queueName}:`, data);
          channel.ack(msg); // Xác nhận đã xử lý thông điệp
        }
      },
      { noAck: false }
    );
  } catch (error) {
    console.error('Error:', error);
  }
};

// Consumer cho nhiệt độ
receiveFromDirectExchange('temperature_queue', 'temperature');

// Consumer cho độ ẩm
receiveFromDirectExchange('humidity_queue', 'humidity');

// Consumer cho pH
receiveFromDirectExchange('ph_queue', 'ph');
