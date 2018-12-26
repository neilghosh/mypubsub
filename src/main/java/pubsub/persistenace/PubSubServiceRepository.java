package pubsub.persistenace;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.springframework.stereotype.Component;

@Component
public class PubSubServiceRepository {
  private final static Logger LOGGER = Logger.getLogger(PubSubServiceRepository.class.getName());

  private static final String TOPICS_DATA_FILE = "data/topics.ser";

  public void persistsTopicSubscriptionMappings(Map<String, Set<String>>  subscribersTopicMap) {
    try {
      ObjectOutputStream stream = new ObjectOutputStream(new FileOutputStream(TOPICS_DATA_FILE));
      stream.writeObject(subscribersTopicMap);
      stream.close();
    } catch (IOException e) {
      LOGGER.severe("Unable to persist topic mappings " + e.getMessage());
    }
  }

  public Map<String, Set<String>> populateTopicSubscriberMap() {

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
}