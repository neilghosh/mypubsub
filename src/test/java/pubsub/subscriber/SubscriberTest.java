package pubsub.subscriber;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.List;

import static org.junit.Assert.assertEquals;

import org.jboss.logging.Logger;
import org.junit.Before;
import org.junit.Test;

import pubsub.Message;
import pubsub.persistenace.SubscriberRepository;
import pubsub.service.PubSubService;

public class SubscriberTest {

  private static final Logger LOGGER = Logger.getLogger(SubscriberTest.class.getName());

  private Subscriber testSubscriber;

  private PubSubService mockPubSubService = mock(PubSubService.class);
  private SubscriberRepository mockSubscriptionRepository = mock(SubscriberRepository.class);


  @Before
  public void setup() {
    testSubscriber = new Subscriber(mockSubscriptionRepository);
  }

  @Test
  public void addToSubscriberMessagesTest() {
    Message message = new Message("someTopic", "someMessage");

    testSubscriber.addToSubscriberMessages(message);

    assertEquals(message, testSubscriber.getSubscriberMessages().peek());
  }

  @Test
  public void addSubscriberTest() {
    testSubscriber.addSubscriber("someTopic", mockPubSubService);

    verify(mockPubSubService).addSubscriber("someTopic", testSubscriber);
  }

  @Test
  public void pullSubscriberMessagesTest() {
    Message testMessage = new Message("someTopic", "someMessage");
    testSubscriber.addToSubscriberMessages(testMessage);

    List<Message> actualMessages = testSubscriber.pullSubscriberMessages();

    assertEquals(testMessage, actualMessages.get(0));
  }
}