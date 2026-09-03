package cc.wechat.observatory;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TenpaySignFallbackTest {
    @Test
    public void matchesNativeTwoKeyTripleDesVector() throws Exception {
        assertEquals(
                "A2DD82831C8F41AE3D27B7AA3FF4A3F50D8216862FE9EAAAC134D50A390B1255",
                TenpaySignFallback.sign("abc"));
    }

    @Test
    public void nullInputMatchesTenpayContract() throws Exception {
        assertNull(TenpaySignFallback.sign(null));
    }

    @Test
    public void rawMd5HexMatchesNativeVector() throws Exception {
        assertEquals(
                "A2DD82831C8F41AE3D27B7AA3FF4A3F50D8216862FE9EAAAC134D50A390B1255",
                TenpaySignFallback.encryptRaw("900150983CD24FB0D6963F7D28E17F72"));
    }

    @Test
    public void appendRequestSignatureSortsAndSkipsEmptyValues() throws Exception {
        Map<String, String> request = new HashMap<>();
        request.put("z", "last");
        request.put("empty", "");
        request.put("a", "first");

        String unsigned = "a=first&z=last";
        assertEquals(unsigned + "&WCPaySign=" + TenpaySignFallback.sign(unsigned),
                TenpaySignFallback.appendRequestSignature(request));
    }
}
