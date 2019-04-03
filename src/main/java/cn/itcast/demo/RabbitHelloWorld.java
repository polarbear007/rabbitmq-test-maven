package cn.itcast.demo;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class RabbitHelloWorld {
	private static final String EXCHANGE_NAME = "exchange_demo";
	private static final String ROUTING_KEY = "routingkey_demo";
	private static final String QUEUE_NAME = "queue_demo";
	private static final String IP_ADDRESS = "192.168.48.129";
	private static final int PORT = 5672;
	
	public static void main(String[] args) throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		// rabbitmq 支持为不同的用户设置不同的虚拟主机（有点像项目名）
		// 比如 admin 用户的虚拟主机为 my_vhost ，那么连接地址其实是 ：
		// 192.168.48.129:5672/my_vhost
		// 比如 root 用户的虚拟主机为 / ,那么连接地址其实是：
		// 192.168.48.129:5672/
		// 如果是 / 的话，那么也可以不用设置
		//factory.setVirtualHost("my_vhost");
		factory.setHost(IP_ADDRESS);
		factory.setPort(PORT);
		factory.setUsername("root");
		factory.setPassword("root");
		Connection conn = factory.newConnection();  // 建立连接对象
		Channel channel = conn.createChannel();   //创建信道
		
		// 通过信道创建一个交换器
		// 类型点对点（direct）, 是否持久（是）， 是否自动删除（否）， 配置参数（没有的话，就写null）
		// exchangeDeclare(String exchange, String type, boolean durable, boolean autoDelete, Map<String, Object> arguments)
		channel.exchangeDeclare(EXCHANGE_NAME, "direct", true, false, null);
		
		// 创建一个队列
		// 是否持久（是）， 是否排他（否），是否自动删除（否）， 配置参数（没有的话，就写 null）
		// queueDeclare(String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments)
		channel.queueDeclare(QUEUE_NAME, true, false, false, null);
		
		// 指定一个路由键，并把队列与交换器进行绑定
		channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
		
		// 发送一条消息
		String message = "Hello world";
		channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
	
		// 关闭资源
		channel.close();
		conn.close();
	}
}
