package pubsub.api;

import org.springframework.web.bind.annotation.RestController;

import pubsub.Message;
import pubsub.persistenace.SubscriberRepository;
import pubsub.publisher.Publisher;
import pubsub.service.PubSubService;
import pubsub.subscriber.Subscriber;

import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@RestController
public class PubSubController {

  @Autowired
  PubSubService pubSubService;


  @RequestMapping(value = "/registerPublisher", method = RequestMethod.POST)
  public String addPublisher(@RequestBody String[] topics) {
    Publisher publisher = new Publisher(Sets.newHashSet(topics));
    publisher.registerPublisher(pubSubService);
    return publisher.getPublisherId();
  }

  @RequestMapping(value = "{publisherId}/publish", method = RequestMethod.POST)
  public String publish(@PathVariable("publisherId") String publisherId, @RequestBody String message) {
    Publisher publisher = pubSubService.getPublisherById(publisherId);

    // Declare Messages and Publish Messages to PubSubService
    for(String topic: publisher.getTopics()){
      Message pubSubMessage = new Message(topic, message);
      publisher.publish(pubSubMessage, pubSubService);
    }
    return message;
  }

  /**
   * Subscribed to the list of topics
   * @param topics
   * @return subscription id
   */
  @RequestMapping(value = "/subscribe", method = RequestMethod.POST)
  public String subscribe(@RequestBody String[] topics) {
    Subscriber subscriber = new Subscriber(new SubscriberRepository());
    for (String topic : topics) {
      subscriber.addSubscriber(topic, pubSubService);
    }

    return subscriber.getSubscriberId();
  }

  /**
   * Gets the messages for a subscriptions across the topics it had subscribed to.
   * @param subscriberId
   * @return
   */
  @RequestMapping("{subscriberId}/pull")
  public String[] pull(@PathVariable("subscriberId") String subscriberId) {
    Subscriber subscriber = pubSubService.getSubscriberById(subscriberId);
    List<Message> pubSubMessages = subscriber.pullSubscriberMessages();
    List<String> messages = Lists.newArrayList();
    for (Message pubSubMessage : pubSubMessages) {
      messages.add(pubSubMessage.getPayload());
    }
    return messages.toArray(new String[0]); //JVM Magic - you don't need the array size :)
  }
}