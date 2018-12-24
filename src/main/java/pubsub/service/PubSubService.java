package pubsub.service;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import pubsub.Message;
import pubsub.subscriber.Subscriber;

@Component
public class PubSubService {
  private final static Logger LOGGER = Logger.getLogger(PubSubService.class.getName());
  public static final String TOPICS_DATA_FILE = "data/topics.ser";

  Map<String, Subscriber> subscribersById;

  // Keeps set of subscriber topic wise, using set to prevent duplicates
  Map<String, Set<String>> subscribersTopicMap;

  // Holds messages published by publishers
  BlockingQueue<Message> messagesQueue;

  public PubSubService() {
    this.subscribersTopicMap = getTopicsFromFile();
    this.subscribersById = new HashMap<>();
    this.messagesQueue = new LinkedBlockingQueue<Message>();
  }

  // Adds message sent by publisher to queue
  public void addMessageToQueue(Message message) {
    messagesQueue.add(message);
    LOGGER.info("Message added. Length of queue is " + messagesQueue.size());
  }

  // Add a new Subscriber for a topic
  public void addSubscriber(String topic, Subscriber subscriber) {
    LOGGER.info("Adding subscriber id " + subscriber.getSubscriberId() + " to topic " + topic);
    subscribersById.put(subscriber.getSubscriberId(), subscriber);

    if (subscribersTopicMap.containsKey(topic)) {
      Set<String> subscriberIds = subscribersTopicMap.get(topic);
      subscriberIds.add(subscriber.getSubscriberId());
      subscribersTopicMap.put(topic, subscriberIds);
    } else {
      Set<String> subscriberIds = new HashSet<>();
      subscriberIds.add(subscriber.getSubscriberId());
      subscribersTopicMap.put(topic, subscriberIds);
    }
    persistsTopicSubscriptionMappings();
  }

  private void persistsTopicSubscriptionMappings() {
    try {
      ObjectOutputStream stream = new ObjectOutputStream(new FileOutputStream(TOPICS_DATA_FILE));
      stream.writeObject(subscribersTopicMap);
      stream.close();
    } catch (IOException e) {
      LOGGER.severe("Unable to persist topic mappings " + e.getMessage());
    }
  }

  private Map<String, Set<String>> getTopicsFromFile() {

    HashMap<String, Set<String>> subscribersTopicMap;
    try {
      ObjectInputStream stream = new ObjectInputStream(new FileInputStream(TOPICS_DATA_FILE));
      subscribersTopicMap = (HashMap<String, Set<String>>) stream.readObject();
      stream.close();
      LOGGER.info("Found Topics " + subscribersTopicMap.size());
    } catch (IOException | ClassNotFoundException e) {
      LOGGER.severe("Unable to load topic mappings " + e.getMessage());
      return new HashMap<String, Set<String>>();
    }
    return subscribersTopicMap;
  }

  // Asynchrounously broadcast new messages added in queue to All subscribers of
  // the topic.
  // messagesQueue will be empty after broadcasting
  @Async
  public void broadcast() {
    LOGGER.info("Broadcasting messages to subscriptions");
    if (messagesQueue.isEmpty()) {
      LOGGER.warning("No messages from publishers to display");
    } else {
      while (!messagesQueue.isEmpty()) {
        Message message = messagesQueue.poll();
        String topic = message.getTopic();

        Set<String> subscriberIdsOfTopic = subscribersTopicMap.get(topic);

        if (subscriberIdsOfTopic == null) {
          LOGGER.warning("Messages in queue but there are no subscription , so they will be lost !!");
        } else {
          for (String subscriberId : subscriberIdsOfTopic) {
            Subscriber subscriber = subscribersById.get(subscriberId);
            // add broadcasted message to subscribers message queue
            if (!subscriber.addToSubscriberMessages(message)) {
              LOGGER.severe("Queue Full for subscription " + subscriberId);
            }
          }
        }
      }
    }
  }

  public Subscriber getSubscriberById(String id) {
    Subscriber subscriber = subscribersById.get(id);
    if (subscriber == null) {
      LOGGER.warning("Subscriber not found in memory , trying to local from file");
      subscriber = Subscriber.loadFromFile(id);
      subscribersById.put(id, subscriber);
    }
    return subscriber;
  }
}