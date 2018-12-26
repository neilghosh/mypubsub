package pubsub.publisher;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Test;

import pubsub.Message;
import pubsub.service.PubSubService;

public class PublisherTest {

  Publisher testPublisher;

  PubSubService mockPubSubService =  mock(PubSubService.class);

  @Before
  public void setup(){
    testPublisher = new Publisher(Sets.newHashSet("someTopic"));
  }

  @Test
  public void testPublish() {
    Message testMessage = new Message("someTopic", "someMessage");

    testPublisher.publish(testMessage, this.mockPubSubService);
  
    verify(this.mockPubSubService).addMessageToQueue(testMessage);
    verify(this.mockPubSubService).broadcast();
  }

  @Test
  public void testRegisterPublisher() {

    testPublisher.registerPublisher(this.mockPubSubService);
  
    verify(this.mockPubSubService).addPublisher(this.testPublisher);
  }
}