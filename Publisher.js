const amqp = require('amqplib');

// Hàm gửi dữ liệu tới Direct Exchange
const sendToDirectExchange = async () => {
  try {
    const connection = await amqp.connect('amqp://localhost'); // Kết nối tới RabbitMQ
    const channel = await connection.createChannel(); // Tạo một kênh giao tiếp

    const exchangeName = 'sensor_direct_exchange'; // Tên Direct Exchange
    await channel.assertExchange(exchangeName, 'direct', { durable: true }); // Tạo Exchange kiểu Direct

    // Dữ liệu cần gửi
    const data = [
      { id: 11, temperature: 30, type: 'temperature' }, // Thông điệp cho consumer xử lý nhiệt độ
      { id: 12, humidity: 60, type: 'humidity' },       // Thông điệp cho consumer xử lý độ ẩm
      { id: 13, pH: 5.0, type: 'ph' }                  // Thông điệp cho consumer xử lý pH
    ];

    // Gửi dữ liệu tới exchange với routing key phù hợp
    for (const item of data) {
      const message = JSON.stringify(item);
      channel.publish(exchangeName, item.type, Buffer.from(message)); // Gửi với routing key là `item.type`
      console.log(`[x] Sent: ${message} with routing key: ${item.type}`);
    }

    setTimeout(() => {
      connection.close(); // Đóng kết nối
    }, 500);
  } catch (error) {
    console.error('Error:', error);
  }
};

sendToDirectExchange();
