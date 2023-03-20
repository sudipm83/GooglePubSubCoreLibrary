package org.example;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.*;
import com.google.pubsub.v1.*;

import java.io.FileInputStream;
import java.io.IOException;

public class PubSubClient {
    private static String PROJECT_ID;
    private static String TOPIC_ID;
    private static String SUBSCRIPTION_ID;

    GoogleCredentials credentials;
    TopicAdminClient topicAdminClient;
    Subscriber subscriber;
    public PubSubClient(String PROJECT_ID, String TOPIC_ID, String SUBSCRIPTION_ID) throws IOException {
        // Create Pub/Sub client object
        this.PROJECT_ID = PROJECT_ID;
        this.TOPIC_ID = TOPIC_ID;
        this.SUBSCRIPTION_ID = SUBSCRIPTION_ID;

        try {
            credentials = GoogleCredentials.fromStream(new FileInputStream("path/to/your/keyfile.json"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {

        }
        topicAdminClient = TopicAdminClient.create(TopicAdminSettings.newBuilder().setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build());
        subscriber = Subscriber.newBuilder(ProjectSubscriptionName.of(PROJECT_ID, SUBSCRIPTION_ID), new MessageReceiverImpl()).setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();

    }

    public Topic getTopic() throws Exception {
        // Get a topic with specific topic and project id
        ProjectTopicName topicName = ProjectTopicName.of(PROJECT_ID, TOPIC_ID);
        if (topicAdminClient.getTopic(topicName) != null) {
            return topicAdminClient.getTopic(topicName);
        }
        return null;
    }

    public void getSubscription() throws InterruptedException {
        // Create a new subscription to the topic, or get an existing subscription by name
        SubscriptionName subscriptionName = SubscriptionName.of(PROJECT_ID, SUBSCRIPTION_ID);
        subscriber.startAsync().awaitRunning();
        // Receive messages from the subscription
        Thread.sleep(10000);
        subscriber.stopAsync();
        subscriber.awaitTerminated();
    }
//    public static void main(String[] args) throws Exception {
//    }

    static class MessageReceiverImpl implements MessageReceiver {
        @Override
        public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
            System.out.println("Received message: " + message.getData().toStringUtf8());
            consumer.ack();
        }
    }


}
