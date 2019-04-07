package cn.itcast.test;

import java.io.IOException;

import org.junit.Test;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;

// 客户端（生产者）请求之前，先声明一个结果队列，并把这个结果队列名保存在消息参数中
// 消费者消费消息以后，先执行任务，然后拿到结果队列名，把处理的结果保存到对应的结果队列中
public class RabbitmqRPC {
	@Test
	public void testClient() throws Exception{
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.131:5672/my_vhost");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();
		
		// 先声明 rpc 交换器和 rpc 队列
		channel.exchangeDeclare("exchange_rpc", BuiltinExchangeType.DIRECT);
		channel.queueDeclare("queue_rpc", true, false, false, null);
		channel.queueBind("queue_rpc", "exchange_rpc", "task.rpc");
		
		// 发送消息以前，我们先声明一个结果队列，因为每个请求都需要新声明一个队列
		// 请求结束以后，就又会删除这个队列，所以我们就用这个无参方法来声明一个
		// 由服务器命名、非持久化、排他的（只有本Connection可以订阅）、自动删除的（无消费订阅后自动删除）的队列
		String resultQueue = channel.queueDeclare().getQueue();
		
		// 然后发送一条消息
		channel.basicPublish("exchange_rpc", 
				             "task.rpc", 
				             new BasicProperties.Builder()
				             	.contentType("text/plain")
				             	.replyTo(resultQueue)
				              	.build(), 
				             "rpc任务".getBytes());
		
		// 发送完消息以后，我们还得订阅这个结果队列，才能拿到这个结果 
		channel.basicConsume(resultQueue, new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
					throws IOException {
				System.out.println(new String(body));
				// 确认消费消息
				channel.basicAck(envelope.getDeliveryTag(), false);
				// 这里停个十秒仅仅是为了让我们有时间去管理页面看一下是否有生成对应的结果队列
				try {
					Thread.sleep(10*1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				// 取消订阅队列，因为队列是自动删除的，所以一取消订阅，这个队列就消失了
				channel.basicCancel(consumerTag);
			}
		});
		
		// 我们要停留一段时间，让远程的服务器消费消息，并返回结果 
		Thread.sleep(10*1000);
		
		channel.close();
		conn.close();
	}
	
	// 服务端的任务就比较简单的，他只要订阅   queue_rpc 队列，然后一直拿消息
	// 消费完消息以后，就把处理结果保存到  消息结构参数中的  replyTo  里面指定的结果队列中
	@Test
	public void testServer() throws Exception{
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.131:5672/my_vhost");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();
		
		channel.basicQos(1);
		channel.basicConsume("queue_rpc", new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
					throws IOException {
				String task = new String(body);
				System.out.println("接收到任务：" + task);
				String result = task + "===> 已经完成";
				// 我们使用默认的交换器， 路由键就使用目标队列的名字，可以通过消息的 replyTo 属性拿到
				channel.basicPublish("", properties.getReplyTo(), MessageProperties.TEXT_PLAIN, result.getBytes());
				// 然后我们确认消费消息，然后啥事也不用管了，等着再处理下一个任务就行了
				channel.basicAck(envelope.getDeliveryTag(), false);
			}
		});
		
		// 我们服务端其实可以一直开着，但是这里就开个 30 秒就好 
		Thread.sleep(300*1000);
		channel.close();
		conn.close();
	}
}
