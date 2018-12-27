package pubsub.persistence;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.springframework.stereotype.Component;

import util.FileUtility;

@Component
public class PubSubServiceRepository {
  private final static Logger LOGGER = Logger.getLogger(PubSubServiceRepository.class.getName());

  private static final String TOPICS_DATA_FILE = "data/topics.ser";

  public void persistsTopicSubscriptionMappings(Map<String, Set<String>> subscribersTopicMap) {
    FileUtility.serialize(subscribersTopicMap, TOPICS_DATA_FILE);
  }

  public Map<String, Set<String>> populateTopicSubscriberMap() {
    Object subscribersTopicMap = FileUtility.deserialize(TOPICS_DATA_FILE);
    if (subscribersTopicMap == null) {
      return new HashMap<String, Set<String>>();
    } else {
      return (HashMap<String, Set<String>>) subscribersTopicMap;
    }
  }
}