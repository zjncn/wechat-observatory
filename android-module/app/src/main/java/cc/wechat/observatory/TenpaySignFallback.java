/*
 * @input Tenpay request source string
 * @output Native-compatible uppercase two-key 3DES signature
 * @position Pure Java fallback for cloned-profile Tenpay JNI resolution failures
 */
package cc.wechat.observatory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

final class TenpaySignFallback {
    private static final String DEFAULT_KEY_HEX = "3ECA2F6FFA6D4952ABBACA5A7B067D23";
    private static final char[] HEX = "0123456789ABCDEF".toCharArray();

    private TenpaySignFallback() {
    }

    static String sign(String source) throws Exception {
        if (source == null) {
            return null;
        }
        byte[] digest = MessageDigest.getInstance("MD5")
                .digest(source.getBytes(StandardCharsets.UTF_8));
        return encryptRaw(upperHex(digest));
    }

    static String appendRequestSignature(Map<String, String> request) throws Exception {
        List<String> keys = new ArrayList<>(request.keySet());
        Collections.sort(keys);
        StringBuilder body = new StringBuilder();
        for (String key : keys) {
            String value = request.get(key);
            if (value == null || value.isEmpty()) {
                continue;
            }
            if (body.length() > 0) {
                body.append('&');
            }
            body.append(key).append('=').append(value);
        }
        String signature = sign(body.toString());
        if (body.length() > 0) {
            body.append('&');
        }
        return body.append("WCPaySign=").append(signature).toString();
    }

    static String encryptRaw(String raw) throws Exception {
        if (raw == null) {
            return null;
        }
        return new String(
                encryptRaw(raw.getBytes(StandardCharsets.UTF_8), null),
                StandardCharsets.UTF_8);
    }

    static byte[] encryptRaw(byte[] raw, byte[] keyHex) throws Exception {
        if (raw == null) {
            return null;
        }
        if ((raw.length & 7) != 0) {
            throw new IllegalArgumentException("Tenpay 3DES source length must be a multiple of 8");
        }
        byte[] key16 = keyHex == null
                ? hexBytes(DEFAULT_KEY_HEX)
                : hexBytes(new String(keyHex, StandardCharsets.US_ASCII));
        if (key16.length != 16) {
            throw new IllegalArgumentException("Tenpay 3DES key must contain 16 bytes");
        }
        byte[] key24 = new byte[24];
        System.arraycopy(key16, 0, key24, 0, key16.length);
        System.arraycopy(key16, 0, key24, key16.length, 8);

        Cipher cipher = Cipher.getInstance("DESede/ECB/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key24, "DESede"));
        return upperHex(cipher.doFinal(raw)).getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] hexBytes(String value) {
        int length = value.length();
        byte[] decoded = new byte[length / 2];
        for (int i = 0; i < length; i += 2) {
            int high = Character.digit(value.charAt(i), 16);
            int low = Character.digit(value.charAt(i + 1), 16);
            if (high < 0 || low < 0) {
                throw new IllegalArgumentException("invalid hexadecimal value");
            }
            decoded[i / 2] = (byte) ((high << 4) | low);
        }
        return decoded;
    }

    private static String upperHex(byte[] value) {
        char[] encoded = new char[value.length * 2];
        for (int i = 0; i < value.length; i++) {
            int unsigned = value[i] & 0xff;
            encoded[i * 2] = HEX[unsigned >>> 4];
            encoded[i * 2 + 1] = HEX[unsigned & 0x0f];
        }
        return new String(encoded);
    }
}
