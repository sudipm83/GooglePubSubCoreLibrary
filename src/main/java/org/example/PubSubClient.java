package org.example;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.SubscriptionName;

import java.io.FileInputStream;

public class PubSubClient {
    private static final String PROJECT_ID = "your-project-id";
    private static final String TOPIC_ID = "your-topic-id";
    private static final String SUBSCRIPTION_ID = "your-subscription-id";

    public static void main(String[] args) throws Exception {
        // Create Pub/Sub client object
        GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream("path/to/your/keyfile.json"));
        TopicAdminClient topicAdminClient = TopicAdminClient.create(TopicAdminSettings.newBuilder().setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build());
        Subscriber subscriber = Subscriber.newBuilder(ProjectSubscriptionName.of(PROJECT_ID, SUBSCRIPTION_ID), new MessageReceiverImpl()).setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();

        // Create a new topic, or get an existing topic by name
        ProjectTopicName topicName = ProjectTopicName.of(PROJECT_ID, TOPIC_ID);
        if (!topicAdminClient.listTopics("projects/" + PROJECT_ID).iterateAll().contains(topicName)) {
            topicAdminClient.createTopic(topicName);
        }

        // Create a new subscription to the topic, or get an existing subscription by name
        SubscriptionName subscriptionName = SubscriptionName.of(PROJECT_ID, SUBSCRIPTION_ID);
        subscriber.startAsync().awaitRunning();

        // Receive messages from the subscription
        Thread.sleep(10000);
        subscriber.stopAsync();
        subscriber.awaitTerminated();
    }

    static class MessageReceiverImpl implements MessageReceiver {
        @Override
        public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
            System.out.println("Received message: " + message.getData().toStringUtf8());
            consumer.ack();
        }
    }
}
