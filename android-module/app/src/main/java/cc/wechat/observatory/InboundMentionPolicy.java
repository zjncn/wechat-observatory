/*
 * @input WeChat message.msgSource XML and the current account wxid
 * @output Exact boolean indicating whether the current account is an @ target
 * @position Privacy-safe inbound mention parser; raw msgSource never leaves the phone
 * @auto-doc Update header and folder INDEX.md when this file changes
 */
package cc.wechat.observatory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class InboundMentionPolicy {
    private static final Pattern AT_USER_LIST = Pattern.compile(
            "<atuserlist(?:\\s[^>]*)?>(.*?)</atuserlist>",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private InboundMentionPolicy() {
    }

    public static boolean mentionsSelf(String msgSource, String selfWxid) {
        String target = selfWxid == null ? "" : selfWxid.trim();
        if (target.isEmpty() || msgSource == null || msgSource.trim().isEmpty()) {
            return false;
        }
        Matcher matcher = AT_USER_LIST.matcher(msgSource);
        if (!matcher.find()) {
            return false;
        }
        String value = matcher.group(1)
                .replace("<![CDATA[", "")
                .replace("]]>", "")
                .replace("&amp;", "&")
                .trim();
        for (String candidate : value.split("[,;\\s]+")) {
            if (target.equals(candidate.trim())) {
                return true;
            }
        }
        return false;
    }
}
