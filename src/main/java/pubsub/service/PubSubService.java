package pubsub.service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Logger;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import pubsub.Message;
import pubsub.subscriber.Subscriber;

@Component
public class PubSubService {

  private final static Logger LOGGER = Logger.getLogger(PubSubService.class.getName());

  Map<String, Subscriber> subscribersById = new HashMap<>();

  // Keeps set of subscriber topic wise, using set to prevent duplicates
  Map<String, Set<Subscriber>> subscribersTopicMap = new HashMap<String, Set<Subscriber>>();

  // Holds messages published by publishers
  Queue<Message> messagesQueue = new LinkedList<Message>();

  // Adds message sent by publisher to queue
  public void addMessageToQueue(Message message) {
    messagesQueue.add(message);
    LOGGER.info("Length of message queue is " + messagesQueue.size());
  }

  // Add a new Subscriber for a topic
  public void addSubscriber(String topic, Subscriber subscriber) {
    LOGGER.info("Subscriber id " + subscriber.getSubscriberId() + " to topic " + topic);
    subscribersById.put(subscriber.getSubscriberId(), subscriber);

    if (subscribersTopicMap.containsKey(topic)) {
      Set<Subscriber> subscribers = subscribersTopicMap.get(topic);
      subscribers.add(subscriber);
      subscribersTopicMap.put(topic, subscribers);
    } else {
      Set<Subscriber> subscribers = new HashSet<Subscriber>();
      subscribers.add(subscriber);
      subscribersTopicMap.put(topic, subscribers);
    }
  }

  // Remove an existing subscriber for a topic
  public void removeSubscriber(String topic, Subscriber subscriber) {

    if (subscribersTopicMap.containsKey(topic)) {
      Set<Subscriber> subscribers = subscribersTopicMap.get(topic);
      subscribers.remove(subscriber);
      subscribersTopicMap.put(topic, subscribers);
    }
  }

  // Broadcast new messages added in queue to All subscribers of the topic.
  // messagesQueue will be empty after broadcasting
  @Async
  public void broadcast() {
    LOGGER.info("Broadcasting messages");
    if (messagesQueue.isEmpty()) {
      System.out.println("No messages from publishers to display");
    } else {
      while (!messagesQueue.isEmpty()) {
        Message message = messagesQueue.remove();
        String topic = message.getTopic();

        Set<Subscriber> subscribersOfTopic = subscribersTopicMap.get(topic);

        if (subscribersOfTopic == null) {
          LOGGER.warning("Messages in queue but there are no subscription , so they will be lost !!");
        } else {
          for (Subscriber subscriber : subscribersOfTopic) {
            // add broadcasted message to subscribers message queue
            List<Message> subscriberMessages = subscriber.getSubscriberMessages();
            subscriberMessages.add(message);
            subscriber.addToSubscriberMessages(subscriberMessages);
          }
        }
      }
    }
  }

  // Remove an existing subscriber for a topic
  public Subscriber getSubscriberById(String id) {
    return subscribersById.get(id);
  }
}