package pubsub.api;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.logging.Logger;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@SpringBootTest
@RunWith(SpringRunner.class)
public class PubSubControllerIntegrationTest {
  private final static Logger LOGGER = Logger.getLogger(PubSubControllerIntegrationTest.class.getName());

  MockMvc mockMvc;

  @Autowired
  protected WebApplicationContext wac;

  @Autowired
  PubSubController pubSubController;

  @Before
  public void setup() throws Exception {
    this.mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
  }

  @Test
  public void testAddPublisher() throws Exception {
    MvcResult result = mockMvc
        .perform(post("/registerPublisher").content("[\"topic1\",\"topic2\"]").contentType(MediaType.APPLICATION_JSON))
        .andDo(MockMvcResultHandlers.print()).andExpect(status().isOk()).andReturn();

    String content = result.getResponse().getContentAsString();
    assertThat(content.length(), is(36));
  }

  @Test
  public void testPublish() throws Exception {
    // Register a publisher

    String publisherId = mockMvc
        .perform(post("/registerPublisher").content("[\"topic1\",\"topic2\"]").contentType(MediaType.APPLICATION_JSON))
        .andReturn().getResponse().getContentAsString();
    ;

    MvcResult result = mockMvc
        .perform(post("/" + publisherId + "/publish").content("Test Message").contentType(MediaType.APPLICATION_JSON))
        .andDo(MockMvcResultHandlers.print()).andExpect(status().isOk()).andReturn();

    String content = result.getResponse().getContentAsString();
    assertThat(content, is("Test Message"));
  }

  @Test
  public void testSubscribe() throws Exception {
    MvcResult result = mockMvc
        .perform(post("/subscribe").content("[\"topic1\",\"topic2\"]").contentType(MediaType.APPLICATION_JSON))
        .andDo(MockMvcResultHandlers.print()).andExpect(status().isOk()).andReturn();

    String content = result.getResponse().getContentAsString();
    assertThat(content.length(), is(36));
  }

  @Test
  public void testPull() throws Exception {
    // Register a publisher
    String publisherId = mockMvc
        .perform(post("/registerPublisher").content("[\"topic1\",\"topic2\"]").contentType(MediaType.APPLICATION_JSON))
        .andReturn().getResponse().getContentAsString();

    // Register a subscriber

    String subscriptionId = mockMvc
        .perform(post("/subscribe").content("[\"topic1\",\"topic2\"]").contentType(MediaType.APPLICATION_JSON))
        .andReturn().getResponse().getContentAsString();

    // Publish a message
    mockMvc
        .perform(post("/" + publisherId + "/publish").content("Test Message").contentType(MediaType.APPLICATION_JSON))
        .andReturn().getResponse().getContentAsString();

    //Wait for the messages to be broadcasted eventually 
    LOGGER.info("Waiting for messages to be eventuallt broadcasted to subscriptions");
    Thread.sleep(2000);
    // Now Pull the message
    MvcResult result = mockMvc.perform(get("/" + subscriptionId + "/pull")).andDo(MockMvcResultHandlers.print())
        .andExpect(status().isOk()).andReturn();

    
    String content = result.getResponse().getContentAsString();
    assertThat(content, is("[\"Test Message\",\"Test Message\"]"));
  }
}