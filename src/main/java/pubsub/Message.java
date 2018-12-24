package pubsub;

import java.util.UUID;

public class Message {

  public static final String DATA_SEPARATOR = ",";

  private String topic;
  private String payload;
  private String messageId;

  public Message() {
  }

  public Message(String topic, String payload) {
    this(topic, payload, UUID.randomUUID().toString());
  }

  public Message(String topic, String payload, String id) {
    this.topic = topic;
    this.payload = payload;
    this.messageId = id;
  }

  public String getMessageId() {
    return messageId;
  }

  private void setMessageId(String messageId) {
    this.messageId = messageId;
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

  public static Message fromString(String line) {
    String[] split = line.split(Message.DATA_SEPARATOR);
    return new Message(split[1], split[2], split[0]);
  }
}