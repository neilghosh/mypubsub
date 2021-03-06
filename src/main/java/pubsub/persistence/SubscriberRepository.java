package pubsub.persistence;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.Lists;

import org.springframework.stereotype.Component;

import pubsub.Message;
import pubsub.subscriber.Subscriber;
import util.FileUtility;

@Component
public class SubscriberRepository {

  private static final Logger LOGGER = Logger.getLogger(SubscriberRepository.class.getName());

  private static final String DATA_DIR = "data";
  private static final String MESSAGE_LOG_PREFIX = DATA_DIR + File.separator + "messageLog-";
  private static final String ACK_LOG_PREFIX = DATA_DIR + File.separator + "ackLog-";

  private boolean firstAckLine = true;
  private boolean firstMessageLine = true;

  private BufferedWriter messageLog; //Stores all the messages received by the subscriber.
  private BufferedWriter ackLog; //Stores the ids of the messages read so far.

  // Sets up the files which backs up the subscription messages and acknowledged
  // which later helps recovering the subscriptions
  public void initializepPersistance(String id, BlockingQueue<Message> messages) {
    // Used for markking the 1st line of file in which case new line is not required
    this.firstAckLine = this.firstMessageLine = messages.size() == 0;
    FileUtility.createDataDir(DATA_DIR);
    messageLog = FileUtility.getWritter(MESSAGE_LOG_PREFIX + id);
    ackLog = FileUtility.getWritter(ACK_LOG_PREFIX + id);
  }

  // Writes all the messages recieved by the subscription into a append only log.
  public synchronized void persistMessage(Message message) {
    this.firstMessageLine = FileUtility.writeToFile(this.firstMessageLine, Lists.newArrayList(message.toString()),
        this.messageLog);
  }

  // Writes a log with acknowledged messages i.e. messages which are read by the
  // user, so that it can later know the pending messages.
  public synchronized void ackMessages(List<Message> messages) {
    List<String> lines = Lists.newArrayList();
    for (Message message : messages) {
      lines.add(message.getMessageId());
    }
    this.firstAckLine = FileUtility.writeToFile(this.firstAckLine, lines, ackLog);
    LOGGER.log(Level.INFO, "Acknowledged {0} messages", lines.size());
  }

  // Load subscription (including its pending messages) from backup file
  // The mechanism is - out of all messages received by the subscription
  // find the messages which are not acknowledged yet.
  public static Subscriber loadFromFile(String id) {
    List<Message> messages = Lists.newArrayList();

    try {
      BufferedReader messageFileReader = FileUtility.getReader(MESSAGE_LOG_PREFIX + id);
      BufferedReader ackFileReader = FileUtility.getReader(ACK_LOG_PREFIX + id);

      String lastAckMessage = null, currentLine = null;
      while ((currentLine = ackFileReader.readLine()) != null) {
        lastAckMessage = currentLine;
      }
      ackFileReader.close();
      LOGGER.info("Last acknowledged message id :" + lastAckMessage);

      boolean ackFound = lastAckMessage == null;
      //Run through the message history look for messages after the last acknowledged 
      //message. 
      while ((currentLine = messageFileReader.readLine()) != null) {
        if (!ackFound) {
          ackFound = currentLine.startsWith(lastAckMessage);
          LOGGER.info("last ack Message found  :" + lastAckMessage);
        } else {
          LOGGER.info("Adding pending message to queue  :" + currentLine);
          messages.add(Message.fromString(currentLine));
        }
      }
      //TODO This is a leaner search could be more efficient.
      //TODO Older acked messages can be archived/deleted from both files.
      messageFileReader.close();
    } catch (IOException e) {
      LOGGER.severe("unable to load subscription from file" + id);
    }
    LOGGER.info("Restoring subscription with queue size  :" + messages.size());
    return new Subscriber(id, new LinkedBlockingQueue<>(messages), new SubscriberRepository());
  }
}