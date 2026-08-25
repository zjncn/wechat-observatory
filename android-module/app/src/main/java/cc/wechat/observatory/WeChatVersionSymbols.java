/*
 * @input Runtime WeChat class-presence probe
 * @output Version-specific aliases for obfuscated WeChat runtime symbols
 * @position Single compatibility boundary for supported Android WeChat versions
 * @auto-doc Update folder INDEX.md when supported versions or aliases change
 */
package cc.wechat.observatory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

final class WeChatVersionSymbols {
    interface ClassProbe {
        boolean isPresent(String className);
    }

    static final class Profile {
        final String version;
        final boolean extensionRegistryRequired;
        final boolean forceServiceInitialization;
        final boolean featureServiceSendStack;
        final boolean originalImageDownloadSupported;
        final boolean videoCrossParamsHasTrailingBoolean;
        final MethodNames methods;
        final FieldNames fields;
        private final Map<String, String> aliases;

        Profile(
                String version,
                boolean extensionRegistryRequired,
                boolean forceServiceInitialization,
                boolean featureServiceSendStack,
                boolean originalImageDownloadSupported,
                boolean videoCrossParamsHasTrailingBoolean,
                MethodNames methods,
                FieldNames fields,
                Map<String, String> aliases) {
            this.version = version;
            this.extensionRegistryRequired = extensionRegistryRequired;
            this.forceServiceInitialization = forceServiceInitialization;
            this.featureServiceSendStack = featureServiceSendStack;
            this.originalImageDownloadSupported = originalImageDownloadSupported;
            this.videoCrossParamsHasTrailingBoolean = videoCrossParamsHasTrailingBoolean;
            this.methods = methods;
            this.fields = fields;
            this.aliases = Collections.unmodifiableMap(new HashMap<>(aliases));
        }

        String className(String baselineName) {
            String alias = aliases.get(baselineName);
            return alias == null ? baselineName : alias;
        }
    }

    static final class FieldNames {
        final String appMsgQuoteItem;
        final String appMsgRecordItem;
        final String miniProgramUsername;
        final String miniProgramPagePath;
        final String miniProgramAppId;
        final String miniProgramIconUrl;
        final String miniProgramSourceName;
        final String miniProgramVersion;
        final String miniProgramType;

        FieldNames(
                String appMsgQuoteItem,
                String appMsgRecordItem,
                String miniProgramUsername,
                String miniProgramPagePath,
                String miniProgramAppId,
                String miniProgramIconUrl,
                String miniProgramSourceName,
                String miniProgramVersion,
                String miniProgramType) {
            this.appMsgQuoteItem = appMsgQuoteItem;
            this.appMsgRecordItem = appMsgRecordItem;
            this.miniProgramUsername = miniProgramUsername;
            this.miniProgramPagePath = miniProgramPagePath;
            this.miniProgramAppId = miniProgramAppId;
            this.miniProgramIconUrl = miniProgramIconUrl;
            this.miniProgramSourceName = miniProgramSourceName;
            this.miniProgramVersion = miniProgramVersion;
            this.miniProgramType = miniProgramType;
        }
    }

    static final class MethodNames {
        final String quoteStorage;
        final String voiceStoragePath;
        final String imageDownloadService;
        final String imageStorage;
        final String imageMessageLookup;
        final String imageHdPath;
        final String imageLargePath;
        final String imageShortSend;
        final String imageLongSend;
        final String videoBuildTask;
        final String voiceSendTask;
        final String emojiManager;
        final String locationBuildTask;
        final String locationSendTask;

        MethodNames(
                String quoteStorage,
                String voiceStoragePath,
                String imageDownloadService,
                String imageStorage,
                String imageMessageLookup,
                String imageHdPath,
                String imageLargePath,
                String imageShortSend,
                String imageLongSend,
                String videoBuildTask,
                String voiceSendTask,
                String emojiManager,
                String locationBuildTask,
                String locationSendTask) {
            this.quoteStorage = quoteStorage;
            this.voiceStoragePath = voiceStoragePath;
            this.imageDownloadService = imageDownloadService;
            this.imageStorage = imageStorage;
            this.imageMessageLookup = imageMessageLookup;
            this.imageHdPath = imageHdPath;
            this.imageLargePath = imageLargePath;
            this.imageShortSend = imageShortSend;
            this.imageLongSend = imageLongSend;
            this.videoBuildTask = videoBuildTask;
            this.voiceSendTask = voiceSendTask;
            this.emojiManager = emojiManager;
            this.locationBuildTask = locationBuildTask;
            this.locationSendTask = locationSendTask;
        }
    }

    private static final Profile WECHAT_8075 = new Profile(
            "8.0.75",
            true,
            true,
            false,
            false,
            false,
            methods("Di", "vj", "", "", "", "", "", "Vi", "aj", "bj", "hj", "Ni", "Zi", "bj"),
            fields("w2", "h0", "j2", "i2", "k2", "B2", "s2", "A2", "l2"),
            Collections.<String, String>emptyMap());

    private static final Profile WECHAT_8076 = new Profile(
            "8.0.76",
            false,
            false,
            true,
            true,
            false,
            methods("lj", "Mj", "lj", "mj", "h2", "tj", "vj", "rj", "vj", "wj", "yj", "mj", "tj", "wj"),
            fields("w2", "h0", "j2", "i2", "k2", "B2", "s2", "A2", "l2"),
            aliases(
                    "i95.n0", "pa5.n0",
                    "i95.y", "pa5.y",
                    "k95.a", "ra5.a",
                    "w11.r0", "y11.r0",
                    "w11.s1", "y11.s1",
                    "aq1.l", "lq1.l",
                    "c01.e2", "e01.e2",
                    "c01.f7", "e01.f7",
                    "c01.h7", "e01.h7",
                    "c01.ia", "e01.ia",
                    "aa0.e", "z90.e",
                    "ui3.b", "wj3.b",
                    "ot0.q", "pt0.r",
                    "tg3.u0", "qh3.u0",
                    "bm5.y", "in5.y",
                    "bm5.f0", "in5.f0",
                    "oi3.g", "qj3.g",
                    "cg0.d", "bg0.d",
                    "dg0.f", "cg0.f",
                    "qi3.b0", "sj3.b0",
                    "qi3.x", "sj3.x",
                    "ut.s0", "xt.s0",
                    "dk5.w", "kl5.w",
                    "vf0.r2", "uf0.s2",
                    "vf0.u2", "uf0.v2",
                    "vf0.i3", "uf0.j3",
                    "vf0.h3", "uf0.i3",
                    "vf0.p2", "uf0.q2",
                    "wf0.b2", "vf0.b2",
                    "y80.q1", "v80.q1",
                    "z80.h0", "w80.h0",
                    "y80.p0", "v80.p0",
                    "r45.uf6", "a65.xh6",
                    "r45.vh4", "a65.oj4",
                    "r45.xz6", "a65.g27",
                    "r15.b", "a35.b",
                    "r15.d", "a35.d",
                    "com.tencent.mm.storage.f9", "com.tencent.mm.storage.e9"));

    private static final Profile WECHAT_8077 = new Profile(
            "8.0.77",
            false,
            false,
            true,
            true,
            true,
            methods("Wi", "Zi", "Wi", "Xi", "k2", "bj", "cj", "bj", "dj", "dj", "hj", "Xi", "bj", "dj"),
            fields("x2", "i0", "k2", "j2", "l2", "C2", "t2", "B2", "m2"),
            aliases(
                    "i95.n0", "sd5.n0",
                    "i95.y", "sd5.y",
                    "k95.a", "ud5.a",
                    "w11.r0", "o41.r0",
                    "w11.s1", "o41.s1",
                    "aq1.l", "ft1.l",
                    "c01.e2", "u21.e2",
                    "c01.f7", "u21.j7",
                    "c01.h7", "u21.l7",
                    "c01.ia", "u21.ba",
                    "aa0.e", "rb0.e",
                    "ui3.b", "sn3.b",
                    "ot0.q", "zv0.r",
                    "tg3.u0", "ll3.u0",
                    "bm5.y", "rq5.x",
                    "bm5.f0", "rq5.e0",
                    "oi3.g", "mn3.g",
                    "cg0.d", "zh0.d",
                    "dg0.f", "ai0.f",
                    "qi3.b0", "on3.b0",
                    "qi3.x", "on3.x",
                    "ut.s0", "eu.q0",
                    "dk5.w", "to5.w",
                    "vf0.r2", "sh0.s2",
                    "vf0.u2", "sh0.v2",
                    "vf0.i3", "sh0.j3",
                    "vf0.h3", "sh0.i3",
                    "vf0.p2", "sh0.q2",
                    "wf0.b2", "th0.b2",
                    "y80.q1", "na0.t1",
                    "z80.h0", "oa0.h0",
                    "y80.p0", "na0.p0",
                    "r45.uf6", "y85.sl6",
                    "r45.vh4", "y85.km4",
                    "r45.xz6", "y85.g67",
                    "r15.b", "y55.d",
                    "r15.d", "y55.f",
                    "qh3.a1", "ll3.a1",
                    "jg0.x", "hi0.x",
                    "h70.y", "z80.y",
                    "h70.w", "z80.w",
                    "qt0.k0", "aw0.k0",
                    "com.tencent.mm.vfs.w6", "com.tencent.mm.vfs.a7",
                    "com.tencent.mm.storage.f9", "com.tencent.mm.storage.e9"));

    private WeChatVersionSymbols() {
    }

    static Profile resolve(ClassProbe probe) {
        if (probe.isPresent(WECHAT_8077.className("i95.n0"))
                && probe.isPresent(WECHAT_8077.className("w11.r0"))) {
            return WECHAT_8077;
        }
        if (probe.isPresent(WECHAT_8076.className("i95.n0"))
                && probe.isPresent(WECHAT_8076.className("w11.r0"))) {
            return WECHAT_8076;
        }
        if (probe.isPresent(WECHAT_8075.className("i95.n0"))
                && probe.isPresent(WECHAT_8075.className("w11.r0"))) {
            return WECHAT_8075;
        }
        throw new IllegalStateException("Unsupported WeChat runtime symbols");
    }

    private static MethodNames methods(
            String quoteStorage,
            String voiceStoragePath,
            String imageDownloadService,
            String imageStorage,
            String imageMessageLookup,
            String imageHdPath,
            String imageLargePath,
            String imageShortSend,
            String imageLongSend,
            String videoBuildTask,
            String voiceSendTask,
            String emojiManager,
            String locationBuildTask,
            String locationSendTask) {
        return new MethodNames(
                quoteStorage,
                voiceStoragePath,
                imageDownloadService,
                imageStorage,
                imageMessageLookup,
                imageHdPath,
                imageLargePath,
                imageShortSend,
                imageLongSend,
                videoBuildTask,
                voiceSendTask,
                emojiManager,
                locationBuildTask,
                locationSendTask);
    }

    private static FieldNames fields(
            String appMsgQuoteItem,
            String appMsgRecordItem,
            String miniProgramUsername,
            String miniProgramPagePath,
            String miniProgramAppId,
            String miniProgramIconUrl,
            String miniProgramSourceName,
            String miniProgramVersion,
            String miniProgramType) {
        return new FieldNames(
                appMsgQuoteItem,
                appMsgRecordItem,
                miniProgramUsername,
                miniProgramPagePath,
                miniProgramAppId,
                miniProgramIconUrl,
                miniProgramSourceName,
                miniProgramVersion,
                miniProgramType);
    }

    private static Map<String, String> aliases(String... entries) {
        if (entries.length % 2 != 0) {
            throw new IllegalArgumentException("Alias entries must be key/value pairs");
        }
        Map<String, String> aliases = new HashMap<>();
        for (int index = 0; index < entries.length; index += 2) {
            aliases.put(entries[index], entries[index + 1]);
        }
        return aliases;
    }
}
