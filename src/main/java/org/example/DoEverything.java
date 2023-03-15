package org.example;
import com.google.cloud.pubsub.v1.*;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.api.gax.rpc.ApiException;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.common.collect.ImmutableList;
import com.google.pubsub.v1.PushConfig;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
public class DoEverything {
    public static void main(String... args) throws Exception {
        // Set the Google Cloud project ID
        String projectId = ServiceOptions.getDefaultProjectId();

        // Create a Google Cloud Pub/Sub topic
        String topicId = "my-topic";
        ProjectTopicName topicName = ProjectTopicName.of(projectId, topicId);
        TopicAdminSettings topicAdminSettings =
                TopicAdminSettings.newBuilder().setCredentialsProvider(
                                () -> GoogleCredentials.getApplicationDefault())
                        .build();
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create(topicAdminSettings)) {
            topicAdminClient.createTopic(topicName);
        }

        // Create a Google Cloud Pub/Sub subscription
        String subscriptionId = "my-subscription";
        ProjectSubscriptionName subscriptionName =
                ProjectSubscriptionName.of(projectId, subscriptionId);
        SubscriptionAdminSettings subscriptionAdminSettings =
                SubscriptionAdminSettings.newBuilder().setCredentialsProvider(
                                () -> GoogleCredentials.getApplicationDefault())
                        .build();
        try (SubscriptionAdminClient subscriptionAdminClient =
                     SubscriptionAdminClient.create(subscriptionAdminSettings)) {
            subscriptionAdminClient.createSubscription(
                    subscriptionName, topicName, PushConfig.getDefaultInstance(), 0);
        }

        // Publish a message to the Google Cloud Pub/Sub topic
        Publisher publisher =
                Publisher.newBuilder(topicName).setCredentialsProvider(
                                () -> GoogleCredentials.getApplicationDefault())
                        .build();
        String message = "Hello, world!";
        ByteString data = ByteString.copyFromUtf8(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
        ApiFuture<String> future = publisher.publish(pubsubMessage);
        String messageId = future.get();
        System.out.println("Published message ID: " + messageId);

        // Consume messages from the Google Cloud Pub/Sub subscription
        Subscriber subscriber =
                Subscriber.newBuilder(subscriptionName, (message, consumer) -> {
                    System.out.println("Received message: " + message.getData().toStringUtf8());
                    consumer.ack();
                }).setCredentialsProvider(() -> GoogleCredentials.getApplicationDefault()).build();
        subscriber.startAsync().awaitRunning();
        subscriber.awaitTerminated(30, TimeUnit.SECONDS);

        // Delete the Google Cloud Pub/Sub subscription and topic
        List<ApiFuture<Void>> futures = ImmutableList.of(
                subscriptionAdminClient.deleteSubscriptionAsync(subscriptionName),
                topicAdminClient.deleteTopicAsync(topicName));
        ApiFutures.allAsList(futures).get();
    }

}