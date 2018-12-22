package pubsub.api;

import org.springframework.web.bind.annotation.RestController;

import pubsub.Message;
import pubsub.publisher.Publisher;
import pubsub.service.PubSubService;
import pubsub.subscriber.Subscriber;

import java.util.List;

import com.google.common.collect.Lists;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@RestController
public class PubSubController {

  @Autowired
  PubSubService pubSubService;

  @RequestMapping(value = "{topic}/publish", method = RequestMethod.POST)
  public String publish(@PathVariable("topic") String topic, @RequestBody String message) {
    Publisher publisher = new Publisher();

    // Declare Messages and Publish Messages to PubSubService
    Message pubSubMessage = new Message(topic, message);

    publisher.publish(pubSubMessage, pubSubService);
    return "published " + pubSubMessage.getPayload();
  }

  @RequestMapping(value = "/subscribe", method = RequestMethod.POST)
  public String subscribe(@RequestBody String[] topics) {
    Subscriber subscriber = new Subscriber();
    for (String topic : topics) {
      subscriber.addSubscriber(topic, pubSubService);
    }

    return subscriber.getSubscriberId();
  }

  @RequestMapping("{subscriberId}/pull")
  public String[] pull(@PathVariable("subscriberId") String subscriberId) {
    Subscriber subscriber = pubSubService.getSubscriberById(subscriberId);
    List<Message> pubSubMessages = subscriber.getSubscriberMessages();
    List<String> messages = Lists.newArrayList();
    for (Message pubSubMessage : pubSubMessages) {
      messages.add(pubSubMessage.getPayload());
    }
    return messages.toArray(new String[0]);
  }
}