package cc.wechat.observatory;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class InboundMentionPolicyTest {
    @Test
    public void detectsCurrentAccountInCdataList() {
        String source = "<msgsource><atuserlist><![CDATA[wxid_other,wxid_self]]></atuserlist></msgsource>";

        assertTrue(InboundMentionPolicy.mentionsSelf(source, "wxid_self"));
    }

    @Test
    public void ignoresMentionsOfOtherMembers() {
        String source = "<msgsource><atuserlist><![CDATA[wxid_other]]></atuserlist></msgsource>";

        assertFalse(InboundMentionPolicy.mentionsSelf(source, "wxid_self"));
    }

    @Test
    public void requiresAnExactWxid() {
        String source = "<msgsource><atuserlist>wxid_self_extra</atuserlist></msgsource>";

        assertFalse(InboundMentionPolicy.mentionsSelf(source, "wxid_self"));
    }
}
