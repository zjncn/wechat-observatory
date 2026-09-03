package cc.wechat.observatory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

public class InboundImagePolicyTest {
    @Test
    public void excludesWechatThumbnailNamesAndKeepsOriginalCandidate() {
        assertEquals(
                Arrays.asList("MicroMsg/profile/image2/9e/12/9e123.jpg", "9e123.jpg"),
                InboundImagePolicy.originalCandidateNames(
                        "MicroMsg/profile/image2/9e/12/th_9e123.jpg?download=1"));
        assertTrue(InboundImagePolicy.isThumbnailPath("/image2/9e/12/th_9e123.jpg"));
        assertTrue(InboundImagePolicy.isThumbnailPath("msgth_9e123"));
        assertTrue(InboundImagePolicy.isThumbnailPath("thumb_9e123.png"));
    }

    @Test
    public void preservesNonThumbnailImageNames() {
        assertEquals(
                Arrays.asList("image2/9e/12/9e123.jpg", "9e123.jpg"),
                InboundImagePolicy.originalCandidateNames("image2/9e/12/9e123.jpg"));
        assertFalse(InboundImagePolicy.isThumbnailPath("image2/9e/12/9e123.jpg"));
    }
}
