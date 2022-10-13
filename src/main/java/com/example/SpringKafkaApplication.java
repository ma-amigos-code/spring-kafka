package com.example;

import com.example.model.Greeting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class SpringKafkaApplication {

	public static void main(String[] args) throws InterruptedException {
		ConfigurableApplicationContext context = SpringApplication.run(SpringKafkaApplication.class, args);

		MessageProducer producer = context.getBean(MessageProducer.class);
		MessageListener listener = context.getBean(MessageListener.class);

		producer.sendMessage("Hello, World!");
		listener.latch.await(10, TimeUnit.SECONDS);

		producer.sendGreetingMessage(new Greeting("Greetings", "World!"));
		listener.greetingLatch.await(10, TimeUnit.SECONDS);

		context.close();
	}

	@Bean
	public MessageProducer messageProducer() {
		return new MessageProducer();
	}

	@Bean
	public MessageListener messageListener() {
		return new MessageListener();
	}

	public static class MessageProducer {

		@Value(value = "${message.topic.name}")
		private String topicName;

		@Value(value = "${greeting.topic.name}")
		private String greetingTopicName;

		@Autowired
		private KafkaTemplate<String, String> kafkaTemplate;

		@Autowired
		private KafkaTemplate<String, Greeting> greetingKafkaTemplate;

		public void sendMessage(String msg) {
			kafkaTemplate.send(topicName, msg);
		}

		public void sendGreetingMessage(Greeting greeting) {
			greetingKafkaTemplate.send(greetingTopicName, greeting);
		}

	}

	public static class MessageListener {

		private CountDownLatch latch = new CountDownLatch(3);

		private CountDownLatch greetingLatch = new CountDownLatch(1);

		@KafkaListener(
				topics = "${message.topic.name}",
				groupId = "foo")
		public void listenGroupFoo(String message) {
			System.out.println("Received Message in group 'foo': " + message);
		}

		@KafkaListener(
				topics = "${greeting.topic.name}",
				groupId = "greeting",
				containerFactory = "greetingKafkaListenerContainerFactory")
		public void greetingListener(Greeting greeting) {
			System.out.println("Received greeting message: " + greeting);
			this.greetingLatch.countDown();
		}
	}

}
