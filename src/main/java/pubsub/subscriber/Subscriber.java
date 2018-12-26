package pubsub.subscriber;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.Lists;

import pubsub.Message;
import pubsub.persistenace.SubscriberRepository;
import pubsub.service.PubSubService;

public class Subscriber {
  private final static Logger LOGGER = Logger.getLogger(Subscriber.class.getName());

  private SubscriberRepository subscriberRepository;
  private BlockingQueue<Message> subscriberMessages;
  private String subscriberId;

  public Subscriber() {
    this(UUID.randomUUID().toString(), new LinkedBlockingQueue<Message>());
  }

  public Subscriber(String id, BlockingQueue<Message> messages) {
    this.subscriberRepository = new SubscriberRepository();
    this.subscriberId = id;
    this.subscriberMessages = messages;
    this.subscriberRepository.initializepPersistance(id, messages);
  }

  public String getSubscriberId() {
    return this.subscriberId;
  }

  public BlockingQueue<Message> getSubscriberMessages() {
    return this.subscriberMessages;
  }

  public boolean addToSubscriberMessages(Message message) {
    this.subscriberRepository.persistMessage(message);
    return this.subscriberMessages.offer(message);
  }

  // Add subscriber with PubSubService for a topic
  public void addSubscriber(String topic, PubSubService pubSubService) {
    pubSubService.addSubscriber(topic, this);
  }

  // Fetches all current messages from subscriptions
  public List<Message> pullSubscriberMessages() {
    List<Message> messages = Lists.newArrayList();
    subscriberMessages.drainTo(messages);
    subscriberRepository.ackMessages(messages);
    LOGGER.log(Level.INFO, "Pulled {0} messages", messages.size());
    return messages;
  }

  public static Subscriber restore(String id) {
    return SubscriberRepository.loadFromFile(id);
  }
}