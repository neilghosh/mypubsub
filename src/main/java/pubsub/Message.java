package pubsub;

import java.io.Serializable;
import java.util.UUID;

public class Message implements Serializable {

  private static final long serialversionUID = 129348938L;
  public static final String DATA_SEPARATOR = ",";

  private String topic;
  private String payload;
  private String messageId;

  public Message() {
  }

  /**
   * @return the messageId
   */
  public String getMessageId() {
    return messageId;
  }

  /**
   * @param messageId the messageId to set
   */
  private void setMessageId(String messageId) {
    this.messageId = messageId;
  }

  public Message(String topic, String payload) {
    this(topic, payload, UUID.randomUUID().toString());
  }

  public Message(String topic, String payload, String id) {
    this.topic = topic;
    this.payload = payload;
    this.messageId = id;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getPayload() {
    return payload;
  }

  public void setPayload(String payload) {
    this.payload = payload;
  }

  public String toString() {
    return messageId + DATA_SEPARATOR + topic + DATA_SEPARATOR + payload;
  }
}