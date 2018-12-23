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

  Map<String, Subscriber> subscribersById = new HashMap<>();

  // Keeps set of subscriber topic wise, using set to prevent duplicates
  Map<String, Set<String>> subscribersTopicMap;

  // Holds messages published by publishers
  BlockingQueue<Message> messagesQueue = new LinkedBlockingQueue<Message>();

  public PubSubService() {
    this.subscribersTopicMap = getTopicsFromFile();
  }


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
      subscribersById.put(subscriber.getSubscriberId(), subscriber);
      Set<String> subscriberIds = subscribersTopicMap.get(topic);
      subscriberIds.add(subscriber.getSubscriberId());
      subscribersTopicMap.put(topic, subscriberIds);
    } else {
      subscribersById.put(subscriber.getSubscriberId(), subscriber);
      Set<String> subscriberIds = new HashSet<>();
      subscriberIds.add(subscriber.getSubscriberId());
      subscribersTopicMap.put(topic, subscriberIds);
    }
    persistsTopic();
  }

  private void persistsTopic() {
    ObjectOutputStream oos;
    try {
      oos = new ObjectOutputStream(new FileOutputStream("TOPICS_DATA_FILE"));
      oos.writeObject(subscribersTopicMap);
      oos.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private Map<String, Set<String>> getTopicsFromFile() {
    ObjectInputStream ois;
    HashMap<String, Set<String>> map;
    try {
      ois = new ObjectInputStream(new FileInputStream("TOPICS_DATA_FILE"));
      map = (HashMap<String, Set<String>>) ois.readObject();
      LOGGER.info("Found Topics "+map.size());
      ois.close();
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
      return new HashMap<String, Set<String>>();
    }
    return map;
  }

  // Remove an existing subscriber for a topic
  public void removeSubscriber(String topic, Subscriber subscriber) {

    if (subscribersTopicMap.containsKey(topic)) {
      Set<String> subscriberIds = subscribersTopicMap.get(topic);
      subscriberIds.remove(subscriber.getSubscriberId());
      subscribersTopicMap.put(topic, subscriberIds);
      subscribersById.remove(subscriber.getSubscriberId());
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
              LOGGER.severe("Queue Full");
            }
          }
        }
      }
    }
  }

  // Remove an existing subscriber for a topic
  public Subscriber getSubscriberById(String id) {
    Subscriber subscriber = subscribersById.get(id);
    if (subscriber == null) {
      LOGGER.warning("Subscriber not found in memory , trying to local from file");
      try {
        subscriber = Subscriber.loadFromFile(id);
        subscribersById.put(id, subscriber);
        
      } catch (IOException e) {
        LOGGER.severe("unable to local from file");
        e.printStackTrace();
        return null;
      }
    }
    return subscriber;
  }
}