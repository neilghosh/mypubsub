package pubsub.subscriber;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;

import com.google.common.collect.Lists;

import pubsub.Message;
import pubsub.service.PubSubService;

public class Subscriber {
  String subscriberId;

  public Subscriber() {
    this.subscriberId = UUID.randomUUID().toString();
  }

  public String getSubscriberId() {
    return this.subscriberId;
  }

  // store all messages received by the subscriber
  private Queue<Message> subscriberMessages = new LinkedList<Message>();

  public List<Message> getSubscriberMessages() {
    List<Message> messages = Lists.newArrayList();
    while (!subscriberMessages.isEmpty()) {
      messages.add(subscriberMessages.remove());
    }
    return messages;
  }

  public void addToSubscriberMessages(List<Message> messages) {
    this.subscriberMessages.addAll(messages);
  }

  // Add subscriber with PubSubService for a topic
  public void addSubscriber(String topic, PubSubService pubSubService) {
    pubSubService.addSubscriber(topic, this);
  }

  // Unsubscribe subscriber with PubSubService for a topic
  public void unSubscribe(String topic, PubSubService pubSubService) {
    pubSubService.removeSubscriber(topic, this);
  }
}