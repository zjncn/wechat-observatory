package cc.wechat.observatory;

import org.junit.Test;

import cc.wechat.observatory.model.MessagePayload;

import static org.junit.Assert.assertTrue;

public class MessagePayloadTest {
    @Test
    public void serializesAtSelfWithoutRawMentionMetadata() {
        MessagePayload payload = new MessagePayload();
        payload.atSelf = true;

        String json = payload.toJson();

        assertTrue(json.contains("\"at_self\":true"));
        assertTrue(!json.contains("msgSource"));
        assertTrue(!json.contains("atuserlist"));
    }
}
