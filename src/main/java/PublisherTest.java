import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PublisherTest {

    private TopicName topicName = TopicName.create("accengage-dev", "phn-topic");
    private Publisher publisher = null;
    private List<ApiFuture<String>> messageIdFutures = new ArrayList<ApiFuture<String>>();

    public void publish() throws Exception {
        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.defaultBuilder(topicName).build();


            List<String> messages = Arrays.asList("first message", "second message");

            // schedule publishing one message at a time : messages get automatically batched
            for (String message : messages) {
                ByteString data = ByteString.copyFromUtf8(message);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

                // Once published, returns a server-assigned message id (unique within the topic)
                ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
                messageIdFutures.add(messageIdFuture);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // wait on any pending publish requests.
            List<String> messageIds = ApiFutures.allAsList(messageIdFutures).get();

            for (String messageId : messageIds) {
                System.out.println("published with message ID: " + messageId);
            }

            if (publisher != null) {
                // When finished with the publisher, shutdown to free up resources.
                publisher.shutdown();
            }
        }
    }

    public static void main (String args[]){
        PublisherTest publisher = new PublisherTest();
        try {
            publisher.publish();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
