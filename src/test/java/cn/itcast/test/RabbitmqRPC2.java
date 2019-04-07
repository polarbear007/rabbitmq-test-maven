package cn.itcast.test;

import java.io.IOException;
import java.util.UUID;

import org.junit.Test;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

// 客户端（生产者）请求之前，先声明一个结果队列，并把这个结果队列名保存在消息参数中
// 消费者消费消息以后，先执行任务，然后拿到结果队列名，把处理的结果保存到对应的结果队列中
public class RabbitmqRPC2 {
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
		
		// 这一次我们声明一个自己命名的、持久化的、不自动删除的队列来保存结果 
		// 这个客户端的所有请求结果都保存在这个队列中
		channel.queueDeclare("queue_rpc_result", true, false, false, null);
		
		// 但是我们需要为每条消息都添加一个全局唯一的 id，
		//  用来标识结果队列中的处理结果跟我们发送的消息的对应关系
		String correlationId = UUID.randomUUID().toString();
		
		// 然后发送一条消息
		channel.basicPublish("exchange_rpc", 
				             "task.rpc", 
				             new BasicProperties.Builder()
				             	.contentType("text/plain")
				             	.replyTo("queue_rpc_result")
				             	.correlationId(correlationId)
				              	.build(), 
				             "rpc任务".getBytes());
		
		// 我们最好还是一次只读取一条结果消息就好了，因为我们要的就只有一条
		channel.basicQos(1);
		// 发送完消息以后，我们还得订阅这个结果队列，才能拿到这个结果 
		channel.basicConsume("queue_rpc_result", new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
					throws IOException {
				// 我们得确保结果队列分配过来的处理结果的correlationId跟我们发送的消息的correlationId是一致的
				// 如果不一致，我们就直接拒绝， 当然拒绝以后，这条消息还是有可能再返回给我们，那就再拒绝。。。
				if(properties.getCorrelationId().equals(correlationId)) {
					// 先打印一下处理的结果 
					System.out.println(new String(body));
					// 然后确认消费这个结果消息
					channel.basicAck(envelope.getDeliveryTag(), false);
					// 拿到结果以后，我们建议还是直接取消订阅，因为如果不取消的话
					// 队列会继续给你投递消息的，但是你结果都拿到了，肯定都是拒绝
					// 【注意】 确认消费消息以后，不要直接就取消订阅，让rabbitMQ留点时间确实从队列中删除这条消息
					//        因为如果你两条命令一起发，很可能rabbitMQ先把订阅取消了，那么这条消息就又被 
					//        放进队列中去了，而这条消息实际已经被消费，也就是其他消费者都消费不了
					//        因此，这绝对是一个灾难！！！
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					channel.basicCancel(consumerTag);
				}else {
					System.out.println("这个结果消息不合适，拒绝消息，放回队列重新投递！");
					channel.basicReject(envelope.getDeliveryTag(), true);
				}
			}
		});
		
		// 我们要停留一段时间，让远程的服务器消费消息，并返回结果 
		Thread.sleep(10*1000);
		
		channel.close();
		conn.close();
	}
	
	// 上面的一个客户端并不能模拟并发问题，我们还是多搞几个客户端来试验吧
	@Test
	public void test() throws Exception {
		for (int i = 0; i < 10; i++) {
			new Thread() {
				public void run() {
					try {
						testClient();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}; 
			}.start();
		}
		
		// 注意： 因为junit 默认会在 test 方法结束以后，关闭所有的子线程，所以
		//       在junit 里面使用多线程的话，必须在最后一句话上面打个断点，然后debug 运行，不让方法结束
		System.out.println();
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
				// 我们还需要指定结果消息的 correlationId ，就使用当前消息的那个 correlationId
				channel.basicPublish("", properties.getReplyTo(), 
						             new BasicProperties.Builder()
						             	.contentType("text/plain")
						             	.correlationId(properties.getCorrelationId())
						             	.build(),
						             result.getBytes());
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
