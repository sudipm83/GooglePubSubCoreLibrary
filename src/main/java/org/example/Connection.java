package org.example;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import java.io.FileInputStream;
import java.io.IOException;

public class Connection {
    String topicName = "my-topic";
    String subscriptionName = "my-subscription";
    String message = "Hello, Pub/Sub!";
    String projectId = "project-name";
    PubSub pubsub;
    public Connection createConnection() throws IOException {
        GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream("path/to/keyfile.json"));
        pubsub = PubSubOptions.newBuilder().setCredentials(credentials).build().getService();
        return null;
    }

    public void createTopic() {
        ProjectTopicName topic = ProjectTopicName.of(projectId, topicName);
        pubsub.create(topic);
    }

    public void createSubscription() {
        ProjectSubscriptionName subscription = ProjectSubscriptionName.of(projectId, subscriptionName);
        pubsub.create(subscription, topic);
    }
    public void publishMessageToTopic() {
        String message = "Hello, Pub/Sub!";
        ByteString data = ByteString.copyFromUtf8(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
        Publisher publisher = pubsub.getPublisher(topic);
        publisher.publish(pubsubMessage);

    }

    public void consumeMessageFromTopic() {
        Subscriber subscriber = pubsub.getSubscriber(subscription, new MessageReceiver() {
            @Override
            public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                String data = message.getData().toStringUtf8();
                System.out.println("Received message: " + data);
                consumer.ack();
            }
        });
        subscriber.startAsync().awaitRunning();

    }




}
