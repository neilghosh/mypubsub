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
import java.util.logging.Level;
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
    this.subscriberId = id;
    this.subscriberMessages = messages;
    setupPersistance(id, messages);
  }

  // Sets up the files which backs up the subscription messages and acknowledged
  // messages
  // which later helps recovering the subscriptions
  private void setupPersistance(String id, BlockingQueue<Message> messages) {
    // Used for markking the 1st line of file in which case new line is not required
    this.firstAckLine = this.firstMessageLine = messages.size() == 0;
    try {
      messageLog = new BufferedWriter(new FileWriter(MESSAGE_LOG_PREFIX + this.subscriberId, true));
      ackLog = new BufferedWriter(new FileWriter(ACK_LOG_PREFIX + this.subscriberId, true));
    } catch (IOException e) {
      LOGGER.severe("Unable to setup message logs " + e.getMessage());
    }
  }

  public String getSubscriberId() {
    return this.subscriberId;
  }

  public boolean addToSubscriberMessages(Message message) {
    persistMessage(message);
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
    ackMessages(messages);
    LOGGER.log(Level.INFO, "Pulled {0} messages", messages.size());
    return messages;
  }

  // Writes all the messages recieved by the subscription into a append only log.
  private synchronized void persistMessage(Message message) {
    this.firstMessageLine = writeToFile(this.firstMessageLine, Lists.newArrayList(message.toString()), this.messageLog);
  }

  // Writes a log with acknowledged messages i.e. messages which are read by the
  // user
  // So that it can later know the pending messages.
  private synchronized void ackMessages(List<Message> messages) {
    List<String> lines = Lists.newArrayList();
    for (Message message : messages) {
      lines.add(message.getMessageId());
    }
    this.firstAckLine = writeToFile(this.firstAckLine, lines, ackLog);
    LOGGER.log(Level.INFO, "Acknowledged {0} messages", lines.size());
  }

  /**
   * 
   * @param isFirstLines If first line of the file has been written We need this
   *                     to know if subsequent lines in the file needs a new line
   * @param lines        Lines to be written
   * @param writter      The file's bufferwritter
   * @return returns if the first line of the file has been written
   */
  private synchronized boolean writeToFile(boolean isFirstLines, List<String> lines, BufferedWriter writter) {
    try {
      for (String line : lines) {
        if (isFirstLines) {
          isFirstLines = false;
        } else {
          writter.newLine();
        }
        writter.write(line);
      }
      writter.flush();
    } catch (IOException e) {
      LOGGER.severe("Unable to write to file " + e.getMessage());
    }
    return isFirstLines;
  }

  // Load subscription (including its pending messages) from backup file
  // The mechanism is - out of all messages received by the subscription
  // find the messages which are not acknowledged yet.
  public static Subscriber loadFromFile(String id) {
    List<Message> messages = Lists.newArrayList();

    try {
      BufferedReader messageFileReader = new BufferedReader(new FileReader(MESSAGE_LOG_PREFIX + id));
      BufferedReader ackFileReader = new BufferedReader(new FileReader(ACK_LOG_PREFIX + id));

      String lastAckMessage = null, sCurrentLine = null;
      while ((sCurrentLine = ackFileReader.readLine()) != null) {
        lastAckMessage = sCurrentLine;
      }
      ackFileReader.close();
      LOGGER.info("Last acknowledged message id :" + lastAckMessage);

      boolean ackFound = lastAckMessage == null;

      while ((sCurrentLine = messageFileReader.readLine()) != null) {
        if (!ackFound) {
          ackFound = sCurrentLine.startsWith(lastAckMessage);
          LOGGER.info("last ack Message found  :" + lastAckMessage);
        } else {
          LOGGER.info("Adding pending message to queue  :" + sCurrentLine);
          messages.add(Message.fromString(sCurrentLine));
        }
      }
      messageFileReader.close();
    } catch (IOException e) {
      LOGGER.severe("unable to load subscription from file" + id);
    }
    LOGGER.info("Restoring subscription with queue size  :" + messages.size());
    return new Subscriber(id, new LinkedBlockingQueue<>(messages));
  }
}