package pubsub.subscriber;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import com.google.common.collect.Lists;

import pubsub.Message;
import pubsub.service.PubSubService;

public class Subscriber {
  private final static Logger LOGGER = Logger.getLogger(Subscriber.class.getName());

  public static final String MESSAGE_LOG_PREFIX = "data/messageLog-";
  public static final String ACK_LOG_PREFIX = "data/ackLog-";

  private BlockingQueue<Message> subscriberMessages;

  boolean firstAckLine = true;
  boolean firstMessageLine = true;

  String subscriberId;
  BufferedWriter messageLog;
  BufferedWriter ackLog;

  public Subscriber() {
    this(UUID.randomUUID().toString(), new LinkedBlockingQueue<Message>());
  }

  public Subscriber(String id, BlockingQueue<Message> messages) {
    this.firstAckLine = this.firstMessageLine = messages.size() == 0;
    this.subscriberId = id;
    this.subscriberMessages = messages;
    try {
      messageLog = new BufferedWriter(new FileWriter(MESSAGE_LOG_PREFIX + this.subscriberId, true));
      ackLog = new BufferedWriter(new FileWriter(ACK_LOG_PREFIX + this.subscriberId, true));
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public String getSubscriberId() {
    return this.subscriberId;
  }

  public List<Message> getSubscriberMessages() {
    List<Message> messages = Lists.newArrayList();
    while (!subscriberMessages.isEmpty()) {
      Message message = subscriberMessages.poll();
      messages.add(message);
    }
    ackMessages(messages);
    return messages;
  }

  public boolean addToSubscriberMessages(Message message) {
    writeToFile(message);
    return this.subscriberMessages.offer(message);
  }

  // Add subscriber with PubSubService for a topic
  public void addSubscriber(String topic, PubSubService pubSubService) {
    pubSubService.addSubscriber(topic, this);
  }

  // Unsubscribe subscriber with PubSubService for a topic
  public void unSubscribe(String topic, PubSubService pubSubService) {
    pubSubService.removeSubscriber(topic, this);
  }

  private synchronized void writeToFile(Message message) {
    try {
      if(firstMessageLine) {
        firstMessageLine = false;
      } else {
        this.messageLog.newLine();
      }
      this.messageLog.write(message.toString());
      this.messageLog.flush();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private synchronized void ackMessages(List<Message> messages) {
    try {
      for (Message message : messages) {
        if(firstAckLine){
          firstAckLine = false;
        } else {
          ackLog.newLine();
        }
        ackLog.write(message.getMessageId());
      }
      ackLog.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static Subscriber loadFromFile(String id) throws IOException {
    BufferedReader messageFileReader = new BufferedReader(new FileReader(MESSAGE_LOG_PREFIX + id));
    BufferedReader ackFileReader = new BufferedReader(new FileReader(ACK_LOG_PREFIX + id));

    String lastAckMessage = null, sCurrentLine = null;
    while ((sCurrentLine = ackFileReader.readLine()) != null) {
      lastAckMessage = sCurrentLine;
    }
    ackFileReader.close();
    LOGGER.info("Last acknowledged message id :" + lastAckMessage);

    boolean ackFound = lastAckMessage == null;
    List<Message> messages = Lists.newArrayList();

    while ((sCurrentLine = messageFileReader.readLine()) != null) {
      if (!ackFound) {
        ackFound = sCurrentLine.startsWith(lastAckMessage);
        LOGGER.info("last ack Message found  :" + lastAckMessage);
      } else {
        LOGGER.info("Adding pending message to queue  :" + sCurrentLine);
        String[] split = sCurrentLine.split(Message.DATA_SEPARATOR);
        messages.add(new Message(split[1], split[2], split[0]));
      }
    }
    messageFileReader.close();
    LOGGER.info("Restoring subscription with queue size   :" + messages.size());
    return new Subscriber(id, new LinkedBlockingQueue<>(messages));
  }
}