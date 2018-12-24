package pubsub.publisher;
import java.util.Set;
import java.util.UUID;

import pubsub.Message;
import pubsub.service.PubSubService;
 
public class Publisher {
  //Publishes new message to PubSubService
  private String publisherId;
  private Set<String> topics; 
  
  public Publisher(Set<String> topics){
    this.publisherId = UUID.randomUUID().toString();
    this.setTopics(topics);
  }

  public Set<String> getTopics() {
    return topics;
  }

  public void setTopics(Set<String> topics) {
    this.topics = topics;
  }

  public String getPublisherId() {
    return publisherId;
  }

  public void publish(Message message, PubSubService pubSubService) {
    pubSubService.addMessageToQueue(message);
    pubSubService.broadcast();
  }
  
  public void registerPublisher(PubSubService pubSubService) {
    pubSubService.addPublisher(this);
	}
}