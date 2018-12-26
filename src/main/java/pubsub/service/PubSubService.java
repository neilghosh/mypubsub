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

import com.google.common.collect.Maps;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import pubsub.Message;
import pubsub.persistenace.PubSubServiceRepository;
import pubsub.publisher.Publisher;
import pubsub.subscriber.Subscriber;

@Component
public class PubSubService {
  private final static Logger LOGGER = Logger.getLogger(PubSubService.class.getName());

  PubSubServiceRepository repository;
  // Keeps publisher to toipics mapping
  Map<String, Publisher> publishersById;

  Map<String, Subscriber> subscribersById;

  // Keeps set of subscriber topic wise, using set to prevent duplicates
  Map<String, Set<String>> subscribersTopicMap;

  // Holds messages published by publishers
  BlockingQueue<Message> messagesQueue;

  @Autowired
  public PubSubService(PubSubServiceRepository repository) {

    this.repository = repository;
    this.publishersById = new HashMap<String, Publisher>();
    this.subscribersTopicMap = repository.populateTopicSubscriberMap();
    this.subscribersById = Maps.newHashMap();
    this.messagesQueue = new LinkedBlockingQueue<Message>();

    populateSubscribers(this.subscribersTopicMap);
  }

  private void populateSubscribers(Map<String, Set<String>> subscribersTopicMap) {
    for (Set<String> subscriptionIds : subscribersTopicMap.values()) {
      for (String subscriptionId : subscriptionIds) {
        this.subscribersById.put(subscriptionId, getSubscriberById(subscriptionId));
      }
    }
    LOGGER.info("Loaded subscriptions " + subscribersById.size());
  }

  public void addPublisher(Publisher publisher) {
    LOGGER.info("Registered Publisher " + publisher.getPublisherId());
    publishersById.put(publisher.getPublisherId(), publisher);
  }

  // Adds message sent by publisher to queue
  public void addMessageToQueue(Message message) {
    LOGGER.info("Adding message to topic " + message.getPayload() + " -- " + message.getTopic());
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
    repository.persistsTopicSubscriptionMappings(this.subscribersTopicMap);
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

  public Publisher getPublisherById(String publisherId) {
    return publishersById.get(publisherId);
  }

  BlockingQueue<Message> getMessageQueue(){
    return this.messagesQueue;
  }

  Map<String, Set<String>> getSubscribersTopicMap() {
    return this.subscribersTopicMap;
  }
}