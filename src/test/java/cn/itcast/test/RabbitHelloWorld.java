package cn.itcast.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.Tx.CommitOk;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.Return;
import com.rabbitmq.client.ReturnCallback;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

public class RabbitHelloWorld {
	private static final String EXCHANGE_NAME = "exchange_demo";
	private static final String ROUTING_KEY = "routingkey_demo";
	private static final String QUEUE_NAME = "queue_demo";
	private static final String IP_ADDRESS = "192.168.48.130";
	private static final int PORT = 5672;

	@Test
	public void testConnection() throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(IP_ADDRESS);
		factory.setPort(PORT);
		factory.setUsername("root");
		factory.setPassword("root");
		Connection conn = factory.newConnection();
		System.out.println(conn);
		conn.close();
	}

	@Test
	public void publisher() throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		// rabbitmq 支持为不同的用户设置不同的虚拟主机（有点像项目名）
		// 比如 admin 用户的虚拟主机为 my_vhost ，那么连接地址其实是 ：
		// 192.168.48.130:5672/my_vhost
		// 比如 root 用户的虚拟主机为 / ,那么连接地址其实是：
		// 192.168.48.130:5672/
		// 如果是 / 的话，那么也可以不用设置
		// factory.setVirtualHost("my_vhost");
		factory.setHost(IP_ADDRESS);
		factory.setPort(PORT);
		factory.setUsername("root");
		factory.setPassword("root");
		Connection conn = factory.newConnection(); // 建立连接对象
		Channel channel = conn.createChannel(); // 创建信道

		// 通过信道创建一个交换器
		// 类型点对点（direct）, 是否持久（是）， 是否自动删除（否）， 配置参数（没有的话，就写null）
		// exchangeDeclare(String exchange, String type, boolean durable, boolean
		// autoDelete, Map<String, Object> arguments)
		channel.exchangeDeclare(EXCHANGE_NAME, "direct", true, false, null);

		// 创建一个队列
		// 是否持久（是）， 是否排他（否），是否自动删除（否）， 配置参数（没有的话，就写 null）
		// queueDeclare(String queue, boolean durable, boolean exclusive, boolean
		// autoDelete, Map<String, Object> arguments)
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

	@Test
	public void recevier() throws Exception {
		Address[] addresses = new Address[] { new Address(IP_ADDRESS, PORT) };

		ConnectionFactory factory = new ConnectionFactory();
		// 目前我们要求发送者跟接收者得是同一个用户
		factory.setUsername("root");
		factory.setPassword("root");

		// 接收方建立连接的方式有点不一样
		Connection conn = factory.newConnection(addresses);

		// 创建信道
		Channel channel = conn.createChannel();

		// 设置客户端最多接收未被ack的消息个数
		channel.basicQos(64);

		// 创建 DefaultConsumer 对象来消费（处理、获取）消息
		Consumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
					throws IOException {
				// 直接打印接收到的消息
				System.out.println("receive message: " + new String(body));
				// 这里直接停个1秒钟，表示处理业务
				try {
					TimeUnit.SECONDS.sleep(1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				channel.basicAck(envelope.getDeliveryTag(), false);
			}
		};

		// 通过consumer 去指定获取并消费 指定队列中的消息
		// 消费过程中，就会回调前面我们重写的 handleDelivery() 方法
		channel.basicConsume(QUEUE_NAME, consumer);

		// 处理一个消息至少要1秒，所以这里大概停个5秒
		TimeUnit.SECONDS.sleep(1000);

		// channel.close();
		// conn.close();
	}

	@Test
	public void testConnection2() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		// 如果把虚拟主机设置成 / ， 那么后面就不需要添加虚拟主机了
		// factory.setUri("amqp://admin:admin@192.168.48.130:5672");
		factory.setUri("amqp://root:root@192.168.48.130:5672/my_vhost");
		Connection conn = factory.newConnection();
		System.out.println(conn);
		conn.close();
	}

	// 错误演示
	@Test
	public void testChannel() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.130:5672/my_vhost");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();

		// 不应该使用 isOpen() 方法来决定是否使用channel 做某件事
		if (channel.isOpen()) {

			// 因为可能会产生竞态条件。
			// 比如说，前面你执行 isOpen() 返回的是 true ，但是在执行下面的语句时
			// connection 已经被关闭了，这个时候再使用 channel 去连接rabbitmq 服务器的话就会报异常
			channel.basicQos(20);
		}
		channel.close();
		conn.close();
	}

	// 正确演示
	@Test
	public void testChannel2() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.130:5672/my_vhost");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();

		try {
			channel.basicQos(20);
		} catch (ShutdownSignalException e) {
			// do something..
		}

		channel.close();
		conn.close();
	}

	@Test
	public void testConsumeMessage1() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.130:5672");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();
		channel.basicQos(3);
		// 设置 消费者 在队列中注册的名字
		// String basicConsume(String queue, boolean autoAck, String consumerTag,
		// Consumer callback)
		channel.basicConsume(QUEUE_NAME, false, "消费者12312", new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
					throws IOException {
				System.out.println(new String(body));
				// channel.basicAck(envelope.getDeliveryTag(), false);
				channel.basicRecover(false);
			}
		});

		// 为了明确看到这个名字，我们固定暂停很久
		Thread.sleep(10000000);
		channel.close();
		conn.close();
	}

	@Test
	public void testConsumeMessage2() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.130:5672");
		Connection conn = factory.newConnection();
		Channel channel1 = conn.createChannel();
		Channel channel2 = conn.createChannel();

		// basicConsume(String queue,
//		                boolean autoAck, 
//		                String consumerTag, 
//		                boolean noLocal, 
//		                boolean exclusive, 
//		                Map<String, Object> arguments, 
//		                Consumer callback) 
		channel1.basicConsume(QUEUE_NAME, false, "消费者1", false, true, null, new DefaultConsumer(channel1));
		channel2.basicConsume(QUEUE_NAME, false, "消费者2", false, false, null, new DefaultConsumer(channel2));

		// 为了明确看到这个名字，我们固定暂停很久
		Thread.sleep(10000000);
		channel1.close();
		channel2.close();
		conn.close();
	}

	@Test
	public void testConsumeMessage3() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.130:5672");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();

		GetResponse response = channel.basicGet(QUEUE_NAME, false);

		System.out.println(response);
		System.out.println(new String(response.getBody()));
		channel.basicAck(response.getEnvelope().getDeliveryTag(), false);

		channel.close();
		conn.close();
	}

	@Test
	public void testShutDownListener() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.130:5672");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();

		// 在这个监听器里面我们可以拿到这个Channel 关闭的原因，异常等信息
		// 我们可以根据这些不同的原因，作出不同的处理方案
		channel.addShutdownListener(new ShutdownListener() {
			@Override
			public void shutdownCompleted(ShutdownSignalException cause) {
				System.out.println("channel 关闭了");
				System.out.println("关闭原因：" + cause.getReason().toString());
			}
		});

		channel.close();
		conn.close();
	}

	@Test
	public void testMandatory() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.130:5672");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();

		// 添加一个 ReturnListener ，用来获取并处理被退回的消息
		channel.addReturnListener(new ReturnCallback() {
			@Override
			public void handle(Return returnMessage) {
				// 拿到被退回消息的消息体部分
				System.out.println(returnMessage.getBody());
				// 拿到是被哪个交换器退回的
				System.out.println(returnMessage.getExchange());
				// 拿到被退回消息的路由键
				System.out.println(returnMessage.getRoutingKey());
				// 拿到退回对应的代码--> 跟http 的状态码差不多
				System.out.println(returnMessage.getReplyCode());
				// 拿到消息的结构化参数
				System.out.println(returnMessage.getProperties());
			}
		});

		// 首先，我们先声明一个不绑定任何队列的交换器
		channel.exchangeDeclare("test_mandatory", BuiltinExchangeType.DIRECT);
		// 然后我们不大片这个交换器绑定任何队列，直接给这个交换器发送消息，并指定 mandatory 为true
		// void basicPublish(String exchange,
//							String routingKey, 
//							boolean mandatory, 
//							BasicProperties props, 
//							byte[] body)
		channel.basicPublish("test_mandatory", "hello", true, MessageProperties.TEXT_PLAIN, "hello".getBytes());

		// 停个十来秒，让rabbitMQ有时间把消息退回
		Thread.sleep(1000 * 10);
		channel.close();
		conn.close();
	}

	@Test
	public void testImmediate() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.130:5672");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();

		// 添加一个 ReturnListener ，用来获取并处理被退回的消息
		channel.addReturnListener(new ReturnCallback() {
			@Override
			public void handle(Return returnMessage) {
				// 拿到被退回消息的消息体部分
				System.out.println(returnMessage.getBody());
				// 拿到是被哪个交换器退回的
				System.out.println(returnMessage.getExchange());
				// 拿到被退回消息的路由键
				System.out.println(returnMessage.getRoutingKey());
				// 拿到退回对应的代码--> 跟http 的状态码差不多
				System.out.println(returnMessage.getReplyCode());
				// 拿到消息的结构化参数
				System.out.println(returnMessage.getProperties());
			}
		});

		// 首先，我们先声明一个交换器
		channel.exchangeDeclare("exchange_immediate", BuiltinExchangeType.DIRECT);
		// 然后，我们再声明一个队列绑定到这个交换器上面
		channel.queueDeclare("queue_immediate", true, false, false, null);
		// 然后，把这个队列绑定到前面的交换器上
		channel.queueBind("queue_immediate", "exchange_immediate", "test.immediate");

		// 然后我们不大片这个交换器绑定任何队列，直接给这个交换器发送消息，
		// 指定 mandatory 为true ， immediate 为true
		// 因为我们已经有绑定队列了，所以 mandatory 参数其实不会起什么作用
		// 但是我们在运行的时候，并没有任何消费者订阅 queue_immediate， 所以 immediate 参数会起作用
//		 void basicPublish(String exchange, 
//				 			String routingKey, 
//				 			boolean mandatory, 
//				 			boolean immediate, 
//				 			BasicProperties props, 
//				 			byte[] body)
		channel.basicPublish("exchange_immediate", "test.immediate", true, true, MessageProperties.TEXT_PLAIN,
				"hello".getBytes());

		// 停个十来秒，让rabbitMQ有时间把消息退回
		Thread.sleep(1000 * 10);
		channel.close();
		conn.close();
	}

	// 统一给一个队列的所有消息设置过期时间
	@Test
	public void testTTL1() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.130:5672");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();

		// 声明一个交换器
		channel.exchangeDeclare("ttl_exchange", BuiltinExchangeType.DIRECT);

		// 声明一个队列，同时给这个队列添加 x-message-ttl 参数
		HashMap<String, Object> args = new HashMap<>();
		args.put("x-message-ttl", 1000 * 6);
		channel.queueDeclare("ttl_queue", true, false, false, args);

		// 再然后，把队列与交换器绑定在一起
		channel.queueBind("ttl_queue", "ttl_exchange", "hello.world");

		// 最我们发送一条消息到交换器上
		channel.basicPublish("ttl_exchange", "hello.world", MessageProperties.TEXT_PLAIN, "hello".getBytes());

		channel.close();
		conn.close();
	}

	// 给某个消息设置独立的过期时间
	@Test
	public void testTTL2() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.130:5672");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();

		// 声明一个交换器，其实完全可以使用以前的那个交换器
		channel.exchangeDeclare("ttl_exchange", BuiltinExchangeType.DIRECT);

		// 然后，声明一个新队列并绑定到ttl_exchage
		channel.queueDeclare("ttl_queue2", true, false, false, null);
		channel.queueBind("ttl_queue2", "ttl_exchange", "hello.world");

		// 最我们发送一条消息到交换器上，但是我们要在这个消息上面添加
		channel.basicPublish("ttl_exchange", "hello.world",
				new BasicProperties.Builder().contentType("text/plain").expiration("10000").build(),
				"hello".getBytes());

		channel.close();
		conn.close();
	}

	@Test
	public void testSaveDeadMessage() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.130:5672");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();

		// 首先，我们应该创建两个交换器，一个是正常的交换器，另一个是投递死信的交换器
		channel.exchangeDeclare("exchange_normal", BuiltinExchangeType.DIRECT);
		// 如果我们只有一个队列来存死信的话，建议使用 fanout 类型的交换器
		channel.exchangeDeclare("exchage_dlx", BuiltinExchangeType.FANOUT);

		// 创建一个队列，这个队列应该带有 x-dead-letter-exchange 的参数
		HashMap<String, Object> args = new HashMap<>();
		args.put("x-dead-letter-exchange", "exchage_dlx");
		channel.queueDeclare("queue_dlx", true, false, false, args);
		// 把这个队列与普通的交换器绑定
		channel.queueBind("queue_dlx", "exchange_normal", "test.dlx");

		// 再然后，我们应该再创建一个普通的队列，用来保存这些死信，并把这个队列跟死信交换器绑定
		channel.queueDeclare("queue_save_dead_message", true, false, false, null);
		// 因为 exchage_dlx 设置的是 fanout 类型，所以根本不需要什么 BindingKey,所以随便搞一个
		channel.queueBind("queue_save_dead_message", "exchage_dlx", "xxx");

		// 发送一个3秒内过期的消息
		channel.basicPublish("exchange_normal", "test.dlx",
				new BasicProperties.Builder().contentType("text/plain").expiration("3000").build(), "hello".getBytes());

		// 休息5秒以后，我们再去那个 queue_save_dead_message 队列里面看看有没有死信信息
		Thread.sleep(5000);
		channel.basicConsume("queue_save_dead_message", new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
					throws IOException {
				System.out.println("从保存死信的队列中读取到一条消息的内容：" + new String(body));

				// 确认消费这条“死信”消息
				channel.basicAck(envelope.getDeliveryTag(), false);
			}
		});

		// 再睡个3秒，保证能读完消息
		// 直接关闭的话，可能会来不及读取消息
		Thread.sleep(3000);
		channel.close();
		conn.close();
	}

	@Test
	public void testSaveDeadMessage2() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.130:5672");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();

		// 普通的交换器，我们就用前面创建的就好了
		channel.exchangeDeclare("exchange_normal", BuiltinExchangeType.DIRECT);
		// 这里我们需要再新建一个死信交换器，类型设置成direct
		channel.exchangeDeclare("exchage_dlx2", BuiltinExchangeType.DIRECT);

		// 创建一个队列，使用 x-dead-letter-exchange 的参数指定保存死信的交换器
		// 使用 x-dead-letter-routing-key 参数指定投递本队列的死信时指定的路由键
		HashMap<String, Object> stuArgs = new HashMap<>();
		stuArgs.put("x-dead-letter-exchange", "exchage_dlx2");
		stuArgs.put("x-dead-letter-routing-key", "student.dead.message");
		channel.queueDeclare("queue_student", true, false, false, stuArgs);
		// 把这个队列与普通的交换器绑定
		channel.queueBind("queue_student", "exchange_normal", "student.message");

		// 因为想要对死信信息进行分类，所以我们还需要再指定一个跟学生队列区别的队列: teacher 队列
		HashMap<String, Object> teacherArgs = new HashMap<>();
		teacherArgs.put("x-dead-letter-exchange", "exchage_dlx2");
		teacherArgs.put("x-dead-letter-routing-key", "teacher.dead.message");
		channel.queueDeclare("queue_teacher", true, false, false, teacherArgs);
		// 把这个队列与普通的交换器绑定
		channel.queueBind("queue_teacher", "exchange_normal", "teacher.message");

		// 再然后，我们应该再创建两个普通的队列，这两个队列分别用来保存学生队列和教师队列产生的死信
		// 先创建一个保存学生队列产生的死信
		channel.queueDeclare("queue_save_student_dead_message", true, false, false, null);
		// 因为 exchage_dlx2 的类型是direct ，所以我们一定要注意 BindingKey 应该跟前面设置的 RoutingKey 一样
		channel.queueBind("queue_save_student_dead_message", "exchage_dlx2", "student.dead.message");

		// 再创建一个队列，用来保存教师队列产生的死信
		channel.queueDeclare("queue_save_teacher_dead_message", true, false, false, null);
		// 因为 exchage_dlx2 的类型是direct ，所以我们一定要注意 BindingKey 应该跟前面设置的 RoutingKey 一样
		channel.queueBind("queue_save_teacher_dead_message", "exchage_dlx2", "teacher.dead.message");

		// 发送一个3秒内过期的消息，到学生队列中
		channel.basicPublish("exchange_normal", "student.message",
				new BasicProperties.Builder().contentType("text/plain").expiration("3000").build(),
				"student hello world".getBytes());

		// 再发送一个3秒内过期的消息，到教师队列中
		channel.basicPublish("exchange_normal", "teacher.message",
				new BasicProperties.Builder().contentType("text/plain").expiration("3000").build(),
				"teacher hello world".getBytes());

		// 休息5秒以后，我们再去那个 queue_save_dead_message 队列里面看看有没有死信信息
		Thread.sleep(5000);
		channel.basicConsume("queue_save_teacher_dead_message", new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
					throws IOException {
				System.out.println("从保存教师死信的队列中读取到一条消息的内容：" + new String(body));

				// 确认消费这条“死信”消息
				channel.basicAck(envelope.getDeliveryTag(), false);
			}
		});

		channel.basicConsume("queue_save_student_dead_message", new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
					throws IOException {
				System.out.println("从保存学生死信的队列中读取到一条消息的内容：" + new String(body));

				// 确认消费这条“死信”消息
				channel.basicAck(envelope.getDeliveryTag(), false);
			}
		});

		// 再睡个3秒，保证能读完消息
		// 直接关闭的话，可能会来不及读取消息
		Thread.sleep(3000);
		channel.close();
		conn.close();
	}

	// 演示一下优先级队列
	@Test
	public void testPriorityQueue() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.130:5672");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();

		// 首先，我们先声明一个普通的交换器
		channel.exchangeDeclare("exchange_priority", BuiltinExchangeType.DIRECT);
		// 然后声明一个队列，添加 x-max-priority 参数
		HashMap<String, Object> args = new HashMap<>();
		args.put("x-max-priority", 5);
		channel.queueDeclare("queue_priority", true, false, false, args);
		// 绑定这个队列和交换器
		channel.queueBind("queue_priority", "exchange_priority", "test.priority");

		// 然后我们就可以发送消息了
		for (int i = 0; i < 20; i++) {
			Integer randNum = new Random().nextInt(6);
			channel.basicPublish("exchange_priority", "test.priority",
					new BasicProperties.Builder().contentType("text/plain").priority(randNum).build(),
					("第" + i + "条数据").getBytes());
		}

		// 再然后，我们就可以接收数据了
		channel.basicConsume("queue_priority", new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
					throws IOException {
				System.out.println("优先级：" + properties.getPriority() + "; 内容：" + new String(body));
				// 确认消费这条消息
				channel.basicAck(envelope.getDeliveryTag(), false);
			}
		});

		// 这里我们需要暂停一下，让消费者能消费消息
		Thread.sleep(10 * 1000);
		channel.close();
		conn.close();
	}

	// 如果消息设置的优先级大于队列的最大优先级，会发生什么情况？
	@Test
	public void testPriorityQueue2() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.130:5672");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();

		// 首先，我们先声明一个普通的交换器
		channel.exchangeDeclare("exchange_priority", BuiltinExchangeType.DIRECT);
		// 然后声明一个队列，添加 x-max-priority 参数
		HashMap<String, Object> args = new HashMap<>();
		args.put("x-max-priority", 5);
		channel.queueDeclare("queue_priority2", true, false, false, args);
		// 绑定这个队列和交换器
		channel.queueBind("queue_priority2", "exchange_priority", "test.priority2");

		// 然后我们就可以发送消息了
		for (int i = 0; i < 20; i++) {
			// 【注意】消息的优先级可能会大于队列的最大优先级
			Integer randNum = new Random().nextInt(10);
			channel.basicPublish("exchange_priority", "test.priority2",
					new BasicProperties.Builder().contentType("text/plain").priority(randNum).build(),
					("第" + i + "条数据").getBytes());
		}

		// 再然后，我们就可以接收数据了
		channel.basicConsume("queue_priority2", new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
					throws IOException {
				System.out.println("优先级：" + properties.getPriority() + "; 内容：" + new String(body));
				// 确认消费这条消息
				channel.basicAck(envelope.getDeliveryTag(), false);
			}
		});

		// 这里我们需要暂停一下，让消费者能消费消息
		Thread.sleep(10 * 1000);
		channel.close();
		conn.close();
	}

	// 如果消息没有设置优先级，但是队列设置了优先级，会发生什么情况？
	@Test
	public void testPriorityQueue3() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.130:5672");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();

		// 首先，我们先声明一个普通的交换器
		channel.exchangeDeclare("exchange_priority", BuiltinExchangeType.DIRECT);
		// 然后声明一个队列，添加 x-max-priority 参数
		HashMap<String, Object> args = new HashMap<>();
		args.put("x-max-priority", 5);
		channel.queueDeclare("queue_priority3", true, false, false, args);
		// 绑定这个队列和交换器
		channel.queueBind("queue_priority3", "exchange_priority", "test.priority3");

		// 然后我们就可以发送消息了
		for (int i = 0; i < 20; i++) {
			// 【注意】注释掉消息的所有优先级
			// Integer randNum = new Random().nextInt(10);
			channel.basicPublish("exchange_priority", "test.priority3",
					new BasicProperties.Builder().contentType("text/plain")
							// .priority(randNum) // 消息直接都不设置优先级了
							.build(),
					("第" + i + "条数据").getBytes());
		}

		// 再然后，我们就可以接收数据了
		channel.basicConsume("queue_priority3", new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
					throws IOException {
				System.out.println("优先级：" + properties.getPriority() + "; 内容：" + new String(body));
				// 确认消费这条消息
				channel.basicAck(envelope.getDeliveryTag(), false);
			}
		});

		// 这里我们需要暂停一下，让消费者能消费消息
		Thread.sleep(10 * 1000);
		channel.close();
		conn.close();
	}

	// 如果消息设置了优先级，但是队列没有设置最大优先级，会发生什么情况？
	@Test
	public void testPriorityQueue4() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.130:5672");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();

		// 首先，我们先声明一个普通的交换器
		channel.exchangeDeclare("exchange_priority", BuiltinExchangeType.DIRECT);
		// 然后声明一个队列，但是不添加 x-max-priority 参数
		// HashMap<String, Object> args = new HashMap<>();
		// args.put("x-max-priority", 5);
		channel.queueDeclare("queue_priority4", true, false, false, null);
		// 绑定这个队列和交换器
		channel.queueBind("queue_priority4", "exchange_priority", "test.priority4");

		// 然后我们就可以发送消息了
		for (int i = 0; i < 20; i++) {
			// 设置消息的优先级
			Integer randNum = new Random().nextInt(5);
			channel.basicPublish("exchange_priority", "test.priority4",
					new BasicProperties.Builder().contentType("text/plain").priority(randNum).build(),
					("第" + i + "条数据").getBytes());
		}

		// 再然后，我们就可以接收数据了
		channel.basicConsume("queue_priority4", new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
					throws IOException {
				System.out.println("优先级：" + properties.getPriority() + "; 内容：" + new String(body));
				// 确认消费这条消息
				channel.basicAck(envelope.getDeliveryTag(), false);
			}
		});

		// 这里我们需要暂停一下，让消费者能消费消息
		Thread.sleep(10 * 1000);
		channel.close();
		conn.close();
	}

	// 延迟队列 + 优先级队列的应用
	// 探究消息变成死信以后，其携带的优先级参数是否还有效

	// 订单消息首先保存在订单队列中，然后5分钟以后自动进入一个延迟队列
	// 进入了延迟队列以后，我们就要对这些订单进行催付
	// 消费延迟队列中的消息，拿到订单号，查询数据库得到这个订单的状态
	// （如果用户在5分钟内付款，我们也不要在订单队列中去遍历查找这个订单消息，让这条消息自动进行延迟队列）
	// （如果订单消息进入延迟队列以后，在我们开始处理订单之前，用户已经 付款，我们也不要主动遍历并查找这个订单消息）
	// （因为rabbitMQ 并不擅长做查询 ，我们把查询放到数据库中来做，或者再用redis 来做）
	// 如果这个订单状态显示已经付款，那么我们就不催了，直接通知确认消费消息即可。
	// 如果这个订单状态显示未付款，那么我们就催付一下（可以短信通知），然后就通知确认消费消息即可。
	// 如果这个订单状态显示用户主动取消，那么我们也不催了，直接确认消费。
	// 如果这个订单状态显示异常状态，比如是非法订单什么的，我们也可以做其他相应的处理，当然也不催付了。。

	// 但是问题的关键是，我们希望根据订单的金额来给订单设置 优先级，订单金额大的优先催付。
	// ===> 思考一上，成为死信保存在延迟队列中的消息，其携带的优先级参数还有效吗？
	@Test
	public void testTTLAndPriority() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.130:5672");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();

		// 首先，我们需要创建一个普通的订单交换器
		channel.exchangeDeclare("exchange_order", BuiltinExchangeType.DIRECT);
		// 然后创建一个用来转发死信的交换器，这个交换器其实就是普通的交换器
		channel.exchangeDeclare("exchange_order_dlx", BuiltinExchangeType.DIRECT);

		// 创建一个保存订单的普通队列
		HashMap<String, Object> orderArgs = new HashMap<>();
		// 只是为了演示方便，设置订单5秒钟以后就自动过期了，然后统一转发给延迟队列
		// 这个队列也不需要什么消费者
		orderArgs.put("x-message-ttl", 5000);
		// 指定消息变成死信以后，要投递到哪一个交换器上
		orderArgs.put("x-dead-letter-exchange", "exchange_order_dlx");
		// 指定投递到交换器上的 RoutingKey
		orderArgs.put("x-dead-letter-routing-key", "order.to.ask.for.payment");
		channel.queueDeclare("queue_new_order", true, false, false, orderArgs);
		channel.queueBind("queue_new_order", "exchange_order", "new.order");

		// 再然后，我们还需要声明一个保存死信的延迟队列
		// 因为我们想要优先催付那些订单金额大的订单，所以我们需要在这个队列设置最大优先级参数
		HashMap<String, Object> payOrderArgs = new HashMap<>();
		payOrderArgs.put("x-max-priority", 5);
		channel.queueDeclare("queue_ask_for_payment_order", true, false, false, payOrderArgs);
		channel.queueBind("queue_ask_for_payment_order", "exchange_order_dlx", "order.to.ask.for.payment");

		// 声明好、绑定好了以后，我们就可以发送消息了
		// 这里我们就不再模拟什么订单金额了，我们直接设置等级。
		// 真实的案例中我们可以写个方法，传入一个订单对象，自动计算出等级什么的
		for (int i = 0; i < 20; i++) {
			int ranNum = new Random().nextInt(6);
			channel.basicPublish("exchange_order", "new.order",
					new BasicProperties.Builder().contentType("text/plain").priority(ranNum).build(),
					("第" + i + "个订单").getBytes());

			channel.basicPublish("exchange_order", "new.order", MessageProperties.PERSISTENT_TEXT_PLAIN,
					"hello".getBytes());
		}

		// 我们应该等待个 7 秒钟，等 那些消息都过期了，再一起消费
		// 因为如果直接消费的话，消费的速度太快，可能就看不到优先级处理的效果了
		Thread.sleep(7 * 1000);

		// 再然后，我们是从 queue_ask_for_payment_order 这个队列中去消费消息的
		// 但是我们最好不要直接消费信息，
		channel.basicConsume("queue_ask_for_payment_order", new DefaultConsumer(channel) {

			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
					throws IOException {
				System.out.println("优先级：" + properties.getPriority() + "; 内容：" + new String(body));
				// 我们就输出一下内容，代表我们处理了这条消息
				// 然后直接就确认消费这条消息了
				channel.basicAck(envelope.getDeliveryTag(), false);
			}
		});

		// 我们应该延迟个 5 秒，让消费者有时间去消费这些过期的消息
		Thread.sleep(5 * 1000);
		channel.close();
		conn.close();
	}

	@Test
	public void testTX() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.130:5672");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();

		// 首先，我们应该先显式地开启事务模式
		channel.txSelect();

		try {
			// 开始发送消息，并把这部分代码放在 try-catch 代码块中
			for (int i = 0; i < 1000; i++) {
				channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, MessageProperties.TEXT_PLAIN,
						String.valueOf(i).getBytes());
			}
			// 人为地抛一下异常，看看效果
			// System.out.println(1/0);
			// 因为我们开启了事务， 所以前面发送的那些消息并不会直接保存到对应 的队列中
			// rabbitmq 暂时把这个消息保存在某个地方，只有当我们提交了事务，才会保存到队列中
			CommitOk txCommit = channel.txCommit();
			System.out.println(txCommit.protocolMethodName());
		} catch (Exception e) {
			// 如果抛异常了，我们就执行事务回滚
			// 如果你不回滚的话，好像查看队列，也不会有什么问题，因为之前发送的那些消息并没保存到队列中
			// 但是你如果不执行回滚，清除那些临时的消息，下次再提交的话，可能会把那些消息也添加进队列中
			// 【注意】我们一般是回滚以后，再重新发送一次，直到成功为止。如果你没有回滚，那么再重新发送的话，
			// 可能就会有一条消息被发送多次的问题。
			System.out.println("抛异常了！");
			channel.txRollback();
		}
		channel.close();
		conn.close();
	}

	// 事务 + 批量操作
	// 插入10万条数据用时：16668ms
	@Test
	public void testTX2() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.130:5672");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();

		long start = System.currentTimeMillis();
		// 首先，我们应该先显式地开启事务模式
		channel.txSelect();
		try {
			// 开始发送消息，并把这部分代码放在 try-catch 代码块中
			for (int i = 0; i < 100000; i++) {
				channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, MessageProperties.TEXT_PLAIN,
						String.valueOf(i).getBytes());
			}
			CommitOk txCommit = channel.txCommit();
			System.out.println(txCommit.protocolMethodName());
		} catch (Exception e) {
			System.out.println("抛异常了！");
			channel.txRollback();
		}
		channel.close();
		conn.close();

		System.out.println("插入10万条数据用时：" + (System.currentTimeMillis() - start) + "ms");
	}

	// 每发送一条数据就开启一次事务
	// 插入10万条数据用时：101232ms
	@Test
	public void testTX3() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.130:5672");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();

		long start = System.currentTimeMillis();

		for (int i = 0; i < 100000; i++) {
			// 每发送一条消息就开启一次事务
			channel.txSelect();
			try {
				// 开始发送消息，并把这部分代码放在 try-catch 代码块中
				channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, MessageProperties.TEXT_PLAIN,
						String.valueOf(i).getBytes());
				CommitOk txCommit = channel.txCommit();
			} catch (Exception e) {
				System.out.println("抛异常了！");
				channel.txRollback();
			}
		}

		channel.close();
		conn.close();

		System.out.println("插入10万条数据用时：" + (System.currentTimeMillis() - start) + "ms");
	}
	
	// 没有任何事务操作
	// 插入10万条数据用时：11401ms
	@Test
	public void testTX4() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.130:5672");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();

		long start = System.currentTimeMillis();

		for (int i = 0; i < 100000; i++) {
			channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, MessageProperties.TEXT_PLAIN,
					String.valueOf(i).getBytes());
		}

		channel.close();
		conn.close();

		System.out.println("插入10万条数据用时：" + (System.currentTimeMillis() - start) + "ms");
	}
	
	// 测试同步的confirm 模式
	// 插入10万条数据用时：52452ms
	@Test
	public void testConfirm() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.130:5672");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();

		long start = System.currentTimeMillis();
		
		// 开启confirm 模式 
		channel.confirmSelect();
		
		for (int i = 0; i < 100000; i++) {
			channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, MessageProperties.TEXT_PLAIN,
					String.valueOf(i).getBytes());
			// 使用channel.waitForConfirms() 等待服务端的回应
			// 如果回复 Basic.Ack 则是true 
			// 如果回复 Basic.Nack 则是false 
			
			// 其实这个方法是一个阻塞式的方法，也就是说如果服务端一直不回复，就会一直卡在这里
			// 为此，我们其实可以给这个方法指定一个时间，时间单位是毫秒
			// 如果等待超过了这个时间，那么这个方法就直接抛异常 IllegalStateException
			if(!channel.waitForConfirms()) {
				// 我们随便打印一条消息，表示我们对发送失败以后的处理
				System.out.println("第" + i + "条消息，发送失败！！！");
			}
		}

		channel.close();
		conn.close();

		System.out.println("插入10万条数据用时：" + (System.currentTimeMillis() - start) + "ms");
	}
	
	// 批量发送消息后，再去确认接收情况
	// 插入10万条数据用时：13708ms
	@Test
	public void testConfirm2() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.130:5672");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();
		
		long start = System.currentTimeMillis();
		
		// 开启confirm 模式 
		channel.confirmSelect();
		
		// 直接发送消息即可
		for (int i = 0; i < 100000; i++) {
			channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, MessageProperties.TEXT_PLAIN,
					String.valueOf(i).getBytes());
			// 本来是想模仿服务端出错，导致某条消息保存失败的，但是这里不知道怎么搞
			// 所以这里手动抛了个异常。 虽然走不到
			if(i == 1000) {
				System.out.println(i/0);
			}
		}
		
		// 我们在十万条消息发送完毕以后，再去确认刚才发送的消息有没有都确实收到了
		// 只要有一条消息返回  Basic.Nack 的话，那么这个方法就会返回 false
		// 只有全部的消息都返回  Basic.Ack ，这个方法才会返回  true
		if(!channel.waitForConfirms()) {
			// 我们随便打印一条消息，表示我们对发送失败以后的处理
			
			// 其实我们根本没办法处理，一来我们不知道是哪条消息出错了
			// 二来这些消息都已经直接写入到队列中，甚至可能部分已经被消费了，我们没办法回滚
			System.out.println("发送失败！！！");
		}

		channel.close();
		conn.close();

		System.out.println("插入10万条数据用时：" + (System.currentTimeMillis() - start) + "ms");
	}
	
	
	@Test
	public void testConfirm3() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.130:5672");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();
		
		long start = System.currentTimeMillis();
		
		// 开启confirm 模式 
		channel.confirmSelect();
		channel.addConfirmListener(new ConfirmListener() {
			// 如果服务端返回 Basic.Nack  的回调函数
			// 这个 deliveryTag 参数其实是从confirm 模式开启以后，对发送消息的计数，起始值是1
			// 比如 回调了 handleNack() 方法，其中的 deliveryTag 值为1 ,则说明我们发送的第1条消息保存失败
			// 比如 回调了 handleAck() 方法，其中的 deliveryTag 值为1 ,则说明我们发送的第2条消息保存成功
			
			// 当然，如果我们发送的消息很多，那么返回的结果可能并不是仅仅是对一条消息的处理结果 ，而是多条消息
			// 具体是看 multiple 参数。 如果这个参数的值为 true ，那么表示 deliveryTag 之前的结果都是一样的
			// 比如第一次回调了handleAck() 方法， 其中的 deliveryTag 值为50， multiple = true
			//   则表示 1-50条消息都已经成功接收
			// 比如第二次回调了 handleNack() 方法， 其中的 deliveryTag 值为70， multiple = false
			//   则表示 51-70 条消息都接收失败
			@Override
			public void handleNack(long deliveryTag, boolean multiple) throws IOException {
				System.out.println("handleNack: deliveryTag: " + deliveryTag);
				System.out.println("handleNack: multiple: " + multiple);
			}
			
			// 如果服务端返回  Basic.Ack 的回调函数
			@Override
			public void handleAck(long deliveryTag, boolean multiple) throws IOException {
				System.out.println("handleAck: deliveryTag: " + deliveryTag);
				System.out.println("handleAck: multiple: " + multiple);
			}
		});
		
		// 直接发送消息即可
		for (int i = 0; i < 100000; i++) {
			channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, MessageProperties.TEXT_PLAIN,
					String.valueOf(i).getBytes());
		}

		// 停个30秒，让客户端有时间接收返回的消息
		Thread.sleep(30*1000);
		channel.close();
		conn.close();

		System.out.println("插入10万条数据用时：" + (System.currentTimeMillis() - start) + "ms");
	}

	// 测试一下备份交换器
	// 备份交换器跟 mandatory 参数的作用有点相似，就是某条消息无法找到对应的队列投递时，会投递给备份交换器
	// 然后备份交换器会尝试根据消息和自身的类型把这些消息投递到与自己绑定的队列上
	// 这样可以防止某些消息因为找不到队列而被丢弃。
	@Test
	public void testAlternateExchange() throws Exception{
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.130:5672");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();
		
		// 声明一个备份交换器，类型最好是使用 fanout , 最好是持久化的
		channel.exchangeDeclare("exchange_alternate", BuiltinExchangeType.FANOUT, true);
		channel.queueDeclare("queue_save_undelivered_message", true, false, false, null);
		channel.queueBind("queue_save_undelivered_message", "exchange_alternate", "any_key");
		
		// 声明一个普通的交换器, 然后通过 alternate-exchange 参数给这个交换器设置一个备份的交换器
		// 为了演示消息无法投递的情况，我们直接不给这个交换器绑定任何队列
		HashMap<String, Object> args = new HashMap<>();
		args.put("alternate-exchange", "exchange_alternate");
		channel.exchangeDeclare("exchange_normal", BuiltinExchangeType.DIRECT, true, false, args);
		
		// 最后，我们就可以来发送一条消息测试一下
		channel.basicPublish("exchange_normal", 
							 "hello.world", 
							 MessageProperties.TEXT_PLAIN, 
							 "hello".getBytes());
		channel.close();
		conn.close();
	}
	
	// 前面我们的事务操作都是针对发送数据
	// 那么读取数据的时候，能不能使用事务操作呢
	// rabbitmq 在读取数据的时候也是可以使用事务进行回滚的
	@Test
	public void testTX5() throws Exception{
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.131:5672/my_vhost");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();
		
		// 开启事务
		channel.txSelect();
		
		GetResponse response = channel.basicGet("myqueue", false);
		byte[] body = response.getBody();
		System.out.println(new String(body));
		
		// 我们试试，发送basicAck ，以后再回滚，看看消息能不能取消消费
		// 结果是可以的
		// 实际上，当我们使用了事务模式，所以的 basicAck 命令会被缓存起来，你正常消费没有问题
		// 但是如果事务最后回滚了，前面的所有 basicAck 命令会全部失效
		channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
		channel.txRollback();
		
		Thread.sleep(20*1000);
		
		channel.close();
		conn.close();
	}
	
	// basicConsume 模式下，也是可以使用事务进行回滚的
	@Test
	public void testTX6() throws Exception{
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.131:5672/my_vhost");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();
		
		// 开启事务
		channel.txSelect();
		
		channel.basicConsume("myqueue", new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
					throws IOException {
				System.out.println(new String(body));
				// 正常确认没有问题，但是事务模式下，这些确认命令并不会马上被 执行
				// 如果最后事务提交了，那么所有的确认命令就会执行
				// 如果最后事务回滚了，那么所有的确认命令就直接失效
				channel.basicAck(envelope.getDeliveryTag(), false);
			}
		});
		
		channel.txRollback();
		
		Thread.sleep(20*1000);
		
		channel.close();
		conn.close();
	}
	
	// 测试一下消费消息，但是没有确认消费，关闭信道还 是关闭连接行为
	// 会让rabbitmq 服务器认为消费者出现故障，把消息重新放回队列
	// ===> 结果是关闭信道以后， rabbitmq 就会把消息重新放回队列
	@Test
	public void test() throws Exception{
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri("amqp://root:root@192.168.48.131:5672/my_vhost");
		Connection conn = factory.newConnection();
		Channel channel = conn.createChannel();
		
		GetResponse response = channel.basicGet("myqueue", false);
		System.out.println(new String(response.getBody()));
		
		Thread.sleep(1000*10);
		
		channel.close();
		System.out.println("信道关闭");
		Thread.sleep(100*1000);
		conn.close();
	}
}
