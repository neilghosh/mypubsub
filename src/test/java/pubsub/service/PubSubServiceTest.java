package pubsub.service;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import pubsub.Message;
import pubsub.persistenace.PubSubServiceRepository;
import pubsub.persistenace.SubscriberRepository;
import pubsub.publisher.Publisher;
import pubsub.subscriber.Subscriber;

public class PubSubServiceTest {

  private final static Logger LOGGER = Logger.getLogger(Subscriber.class.getName());

  PubSubServiceRepository mockPubSubServiceRepository = mock(PubSubServiceRepository.class);
  SubscriberRepository subscriberRepository = mock(SubscriberRepository.class);

  PubSubService testPubsubService;

  @Before
  public void setup() {
    cleanData();
    testPubsubService = new PubSubService(new PubSubServiceRepository());
  }

  @Test
  public void addPublisherTest() {
    Publisher publisher = new Publisher(Sets.newHashSet());

    testPubsubService.addPublisher(publisher);

    assertNotNull(testPubsubService.getPublisherById(publisher.getPublisherId()));
  }

  @Test
  public void addMessageToQueueTest() {
    Message message = new Message("someTopic", "someMessage");

    testPubsubService.addMessageToQueue(message);

    assertThat(testPubsubService.getMessageQueue().size(), is(1));
  }

  @Test
  public void addSubscriberTest() {
    Subscriber subscriber = new Subscriber("someSubscriber",
        new LinkedBlockingQueue<Message>(Lists.newArrayList(new Message("someTopic", "someMessage"))),
        subscriberRepository);

    testPubsubService.addSubscriber("someTopic", subscriber);

    assertNotNull(testPubsubService.getSubscriberById("someSubscriber"));
    assertEquals(testPubsubService.getSubscribersTopicMap().get("someTopic"), Sets.newHashSet("someSubscriber"));
  }

  @Test
  public void broadcastTest() {
    // Add a subscriber
    Subscriber subscriber = new Subscriber(subscriberRepository);
    testPubsubService.addSubscriber("someTopic", subscriber);

    // Add a message
    Message message = new Message("someTopic", "someMessage");
    testPubsubService.addMessageToQueue(message);

    testPubsubService.broadcast();

    assertEquals(message, subscriber.getSubscriberMessages().peek());
  }

  @After
  public void tearDown() {
    cleanData();
  }

  private void cleanData() {
    LOGGER.info("Test directory --- " + new File("data").getAbsolutePath());
    // Remove all test data
    try {
      for (File file : new File("data").listFiles()) {
        if (!file.getName().startsWith(".")) {
          file.delete();
        }
      }
    } catch (Exception e) {
      LOGGER.severe("Unable to clean " + new File("data").getAbsolutePath());
    }
  }
}