package cc.wechat.observatory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

public class WeChatVersionSymbolsTest {
    @Test
    public void resolvesWechat8077BeforeStale8076Lookalikes() {
        WeChatVersionSymbols.Profile profile = resolveWith(
                "sd5.n0", "o41.r0",
                "pa5.n0", "y11.r0");

        assertEquals("8.0.77", profile.version);
        assertEquals("sd5.n0", profile.className("i95.n0"));
        assertEquals("sd5.y", profile.className("i95.y"));
        assertEquals("ud5.a", profile.className("k95.a"));
        assertEquals("o41.r0", profile.className("w11.r0"));
        assertEquals("o41.s1", profile.className("w11.s1"));
        assertEquals("ft1.l", profile.className("aq1.l"));
        assertEquals("u21.e2", profile.className("c01.e2"));
        assertEquals("u21.j7", profile.className("c01.f7"));
        assertEquals("u21.l7", profile.className("c01.h7"));
        assertEquals("u21.ba", profile.className("c01.ia"));
        assertEquals("rb0.e", profile.className("aa0.e"));
        assertEquals("sn3.b", profile.className("ui3.b"));
        assertEquals("zv0.r", profile.className("ot0.q"));
        assertEquals("ll3.u0", profile.className("tg3.u0"));
        assertEquals("rq5.x", profile.className("bm5.y"));
        assertEquals("rq5.e0", profile.className("bm5.f0"));
        assertEquals("mn3.g", profile.className("oi3.g"));
        assertEquals("zh0.d", profile.className("cg0.d"));
        assertEquals("ai0.f", profile.className("dg0.f"));
        assertEquals("on3.b0", profile.className("qi3.b0"));
        assertEquals("on3.x", profile.className("qi3.x"));
        assertEquals("eu.q0", profile.className("ut.s0"));
        assertEquals("to5.w", profile.className("dk5.w"));
        assertEquals("sh0.s2", profile.className("vf0.r2"));
        assertEquals("sh0.v2", profile.className("vf0.u2"));
        assertEquals("sh0.j3", profile.className("vf0.i3"));
        assertEquals("sh0.i3", profile.className("vf0.h3"));
        assertEquals("sh0.q2", profile.className("vf0.p2"));
        assertEquals("th0.b2", profile.className("wf0.b2"));
        assertEquals("na0.t1", profile.className("y80.q1"));
        assertEquals("oa0.h0", profile.className("z80.h0"));
        assertEquals("na0.p0", profile.className("y80.p0"));
        assertEquals("y85.sl6", profile.className("r45.uf6"));
        assertEquals("y85.km4", profile.className("r45.vh4"));
        assertEquals("y85.g67", profile.className("r45.xz6"));
        assertEquals("y55.d", profile.className("r15.b"));
        assertEquals("y55.f", profile.className("r15.d"));
        assertEquals("ll3.a1", profile.className("qh3.a1"));
        assertEquals("hi0.x", profile.className("jg0.x"));
        assertEquals("z80.y", profile.className("h70.y"));
        assertEquals("z80.w", profile.className("h70.w"));
        assertEquals("aw0.k0", profile.className("qt0.k0"));
        assertEquals("com.tencent.mm.vfs.a7", profile.className("com.tencent.mm.vfs.w6"));
        assertEquals("com.tencent.mm.storage.e9", profile.className("com.tencent.mm.storage.f9"));
        assertEquals("Wi", profile.methods.quoteStorage);
        assertEquals("Zi", profile.methods.voiceStoragePath);
        assertEquals("Wi", profile.methods.imageDownloadService);
        assertEquals("Xi", profile.methods.imageStorage);
        assertEquals("k2", profile.methods.imageMessageLookup);
        assertEquals("bj", profile.methods.imageHdPath);
        assertEquals("cj", profile.methods.imageLargePath);
        assertEquals("bj", profile.methods.imageShortSend);
        assertEquals("dj", profile.methods.imageLongSend);
        assertEquals("dj", profile.methods.videoBuildTask);
        assertEquals("hj", profile.methods.voiceSendTask);
        assertEquals("Xi", profile.methods.emojiManager);
        assertEquals("bj", profile.methods.locationBuildTask);
        assertEquals("dj", profile.methods.locationSendTask);
        assertEquals("x2", profile.fields.appMsgQuoteItem);
        assertEquals("i0", profile.fields.appMsgRecordItem);
        assertEquals("k2", profile.fields.miniProgramUsername);
        assertEquals("j2", profile.fields.miniProgramPagePath);
        assertEquals("l2", profile.fields.miniProgramAppId);
        assertEquals("C2", profile.fields.miniProgramIconUrl);
        assertEquals("t2", profile.fields.miniProgramSourceName);
        assertEquals("B2", profile.fields.miniProgramVersion);
        assertEquals("m2", profile.fields.miniProgramType);
        assertFalse(profile.extensionRegistryRequired);
        assertFalse(profile.forceServiceInitialization);
        assertTrue(profile.featureServiceSendStack);
        assertTrue(profile.originalImageDownloadSupported);
        assertTrue(profile.videoCrossParamsHasTrailingBoolean);
    }

    @Test
    public void resolvesWechat8076AndMapsVerifiedSymbols() {
        WeChatVersionSymbols.Profile profile = resolveWith("pa5.n0", "y11.r0");

        assertEquals("8.0.76", profile.version);
        assertEquals("pa5.n0", profile.className("i95.n0"));
        assertEquals("pa5.y", profile.className("i95.y"));
        assertEquals("ra5.a", profile.className("k95.a"));
        assertEquals("y11.r0", profile.className("w11.r0"));
        assertEquals("y11.s1", profile.className("w11.s1"));
        assertEquals("lq1.l", profile.className("aq1.l"));
        assertEquals("e01.e2", profile.className("c01.e2"));
        assertEquals("e01.f7", profile.className("c01.f7"));
        assertEquals("e01.h7", profile.className("c01.h7"));
        assertEquals("e01.ia", profile.className("c01.ia"));
        assertEquals("pt0.r", profile.className("ot0.q"));
        assertEquals("z90.e", profile.className("aa0.e"));
        assertEquals("wj3.b", profile.className("ui3.b"));
        assertEquals("qh3.u0", profile.className("tg3.u0"));
        assertEquals("in5.y", profile.className("bm5.y"));
        assertEquals("in5.f0", profile.className("bm5.f0"));
        assertEquals("qj3.g", profile.className("oi3.g"));
        assertEquals("bg0.d", profile.className("cg0.d"));
        assertEquals("cg0.f", profile.className("dg0.f"));
        assertEquals("sj3.b0", profile.className("qi3.b0"));
        assertEquals("sj3.x", profile.className("qi3.x"));
        assertEquals("xt.s0", profile.className("ut.s0"));
        assertEquals("kl5.w", profile.className("dk5.w"));
        assertEquals("uf0.s2", profile.className("vf0.r2"));
        assertEquals("uf0.v2", profile.className("vf0.u2"));
        assertEquals("uf0.j3", profile.className("vf0.i3"));
        assertEquals("uf0.i3", profile.className("vf0.h3"));
        assertEquals("uf0.q2", profile.className("vf0.p2"));
        assertEquals("vf0.b2", profile.className("wf0.b2"));
        assertEquals("v80.q1", profile.className("y80.q1"));
        assertEquals("w80.h0", profile.className("z80.h0"));
        assertEquals("v80.p0", profile.className("y80.p0"));
        assertEquals("a65.xh6", profile.className("r45.uf6"));
        assertEquals("a65.oj4", profile.className("r45.vh4"));
        assertEquals("a65.g27", profile.className("r45.xz6"));
        assertEquals("a35.b", profile.className("r15.b"));
        assertEquals("a35.d", profile.className("r15.d"));
        assertEquals("com.tencent.mm.storage.e9", profile.className("com.tencent.mm.storage.f9"));
        assertFalse(profile.extensionRegistryRequired);
        assertFalse(profile.forceServiceInitialization);
        assertTrue(profile.featureServiceSendStack);
        assertTrue(profile.originalImageDownloadSupported);
        assertFalse(profile.videoCrossParamsHasTrailingBoolean);
        assertEquals("Mj", profile.methods.voiceStoragePath);
        assertEquals("rj", profile.methods.imageShortSend);
        assertEquals("wj", profile.methods.videoBuildTask);
        assertEquals("yj", profile.methods.voiceSendTask);
        assertEquals("mj", profile.methods.emojiManager);
        assertEquals("w2", profile.fields.appMsgQuoteItem);
        assertEquals("h0", profile.fields.appMsgRecordItem);
        assertEquals("j2", profile.fields.miniProgramUsername);
    }

    @Test
    public void keepsWechat8075BaselineSymbolsUnchanged() {
        WeChatVersionSymbols.Profile profile = resolveWith("i95.n0", "w11.r0");

        assertEquals("8.0.75", profile.version);
        assertEquals("i95.n0", profile.className("i95.n0"));
        assertEquals("w11.s1", profile.className("w11.s1"));
        assertTrue(profile.extensionRegistryRequired);
        assertTrue(profile.forceServiceInitialization);
        assertFalse(profile.featureServiceSendStack);
        assertFalse(profile.originalImageDownloadSupported);
        assertFalse(profile.videoCrossParamsHasTrailingBoolean);
        assertEquals("vj", profile.methods.voiceStoragePath);
        assertEquals("Vi", profile.methods.imageShortSend);
        assertEquals("w2", profile.fields.appMsgQuoteItem);
    }

    @Test(expected = IllegalStateException.class)
    public void rejectsUnknownWechatRuntime() {
        resolveWith("some.other.Class");
    }

    private static WeChatVersionSymbols.Profile resolveWith(String... names) {
        final Set<String> available = new HashSet<>(Arrays.asList(names));
        return WeChatVersionSymbols.resolve(new WeChatVersionSymbols.ClassProbe() {
            @Override
            public boolean isPresent(String className) {
                return available.contains(className);
            }
        });
    }
}
