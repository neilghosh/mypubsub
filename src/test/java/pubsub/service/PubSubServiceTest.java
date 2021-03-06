package pubsub.service;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import pubsub.Message;
import pubsub.persistence.PubSubServiceRepository;
import pubsub.persistence.SubscriberRepository;
import pubsub.publisher.Publisher;
import pubsub.subscriber.Subscriber;

public class PubSubServiceTest {

  private final static Logger LOGGER = Logger.getLogger(Subscriber.class.getName());

  PubSubServiceRepository mockPubSubServiceRepository = mock(PubSubServiceRepository.class);
  SubscriberRepository mockSubscriberRepository = mock(SubscriberRepository.class);

  PubSubService testPubsubService;

  @Before
  public void setup() {
    //FileUtility.cleanData();
    testPubsubService = new PubSubService(mockPubSubServiceRepository);
  }

  @Test
  public void testAddPublisher() {
    Publisher publisher = new Publisher(Sets.newHashSet());

    testPubsubService.addPublisher(publisher);

    assertNotNull(testPubsubService.getPublisherById(publisher.getPublisherId()));
  }

  @Test
  public void testAddMessageToQueue() {
    Message message = new Message("someTopic", "someMessage");

    testPubsubService.addMessageToQueue(message);

    assertThat(testPubsubService.getMessageQueue().size(), is(1));
  }

  @Test
  public void testAddSubscriber() {
    Subscriber subscriber = new Subscriber("someSubscriber",
        new LinkedBlockingQueue<Message>(Lists.newArrayList(new Message("someTopic", "someMessage"))),
        mockSubscriberRepository);

    testPubsubService.addSubscriber("someTopic", subscriber);

    assertNotNull(testPubsubService.getSubscriberById("someSubscriber"));
    assertEquals(testPubsubService.getSubscribersTopicMap().get("someTopic"), Sets.newHashSet("someSubscriber"));
  }

  @Test
  public void testBroadcast() {
    // Add a subscriber
    Subscriber subscriber = new Subscriber(mockSubscriberRepository);
    testPubsubService.addSubscriber("someTopic", subscriber);

    // Add a message
    Message message = new Message("someTopic", "someMessage");
    testPubsubService.addMessageToQueue(message);

    testPubsubService.broadcast();

    assertEquals(message, subscriber.getSubscriberMessages().peek());
  }

  @After
  public void tearDown() {
    //FileUtility.cleanData();
  }
}