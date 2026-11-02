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

    /**
     * WeChat 8.0.78's R8 build reassigned every short class name from 8.0.77 to a
     * different (often unrelated) class instead of keeping any of them stable, so
     * this profile is seeded from WECHAT_8077's aliases as a starting point rather
     * than a verified mapping. Only the two resolve() probe classes below
     * ("i95.n0" -> ph5.n0, "w11.r0" -> v51.r0) have been re-located and confirmed
     * structurally (same field/method shapes) via androguard against the real
     * 8.0.78 dex; every other alias, method name and field name copied from
     * WECHAT_8077 is unverified for 8.0.78 and should be treated as a placeholder
     * to be corrected the first time the feature that uses it is exercised and
     * fails (same error-driven approach used for the earlier 8.0.76/8.0.77 work).
     */
    private static final Profile WECHAT_8078 = new Profile(
            "8.0.78",
            false,
            false,
            true,
            true,
            true,
            methods("Wi", "Ej", "ej", "fj", "k2", "bj", "cj", "ij", "kj", "lj", "pj", "fj", "jj", "lj"),
            fields("x2", "i0", "k2", "j2", "l2", "C2", "t2", "B2", "m2"),
            aliases(
                    "i95.n0", "ph5.n0",
                    "i95.y", "ph5.y",
                    "k95.a", "rh5.a",
                    "w11.r0", "v51.r0",
                    "w11.s1", "v51.s1",
                    "aq1.l", "vu1.l",
                    "c01.e2", "b41.d2",
                    "c01.f7", "b41.i7",
                    "c01.h7", "b41.k7",
                    "c01.ia", "u21.ba",
                    "aa0.e", "rb0.e",
                    "ui3.b", "sn3.b",
                    "ot0.q", "dx0.r",
                    "tg3.u0", "rn3.u0",
                    "bm5.y", "ou5.x",
                    "bm5.f0", "ou5.e0",
                    "oi3.g", "sp3.g",
                    "cg0.d", "xi0.d",
                    "dg0.f", "yi0.f",
                    "qi3.b0", "up3.c0",
                    "qi3.x", "up3.y",
                    "ut.s0", "gu.q0",
                    "dk5.w", "qs5.w",
                    "vf0.r2", "qi0.t2",
                    "vf0.u2", "qi0.w2",
                    "vf0.i3", "qi0.k3",
                    "vf0.h3", "qi0.j3",
                    "vf0.p2", "qi0.r2",
                    "wf0.b2", "ri0.b2",
                    "y80.q1", "kb0.t1",
                    "z80.h0", "lb0.h0",
                    "y80.p0", "kb0.p0",
                    "r45.uf6", "pc5.qn6",
                    "r45.vh4", "pc5.qn4",
                    "r45.xz6", "pc5.p87",
                    "r15.b", "p95.d",
                    "r15.d", "p95.f",
                    "qh3.a1", "rn3.a1",
                    "jg0.x", "fj0.x",
                    "h70.y", "w90.y",
                    "h70.w", "w90.w",
                    "qt0.k0", "ex0.k0",
                    "com.tencent.mm.vfs.w6", "com.tencent.mm.vfs.a7",
                    "com.tencent.mm.storage.f9", "com.tencent.mm.storage.e9"));

    /**
     * WeChat 8.0.79 reassigned nearly every short obfuscated class name again, same as
     * every prior point release. Resolved via structural shape-matching (method/field
     * signatures, superclass, interfaces) between the 8.0.78 and 8.0.79 dex files rather
     * than purely by re-running the feature and watching it fail, since that tooling
     * (dex_shape.py-style class-shape index + similarity ranking, cross-checked against
     * jadx-decompiled method bodies for the ones with ambiguous/duplicate signatures)
     * now exists. The two resolve() probe classes, the image/voice/video download
     * feature services (dg0.f/h70.y/h70.w/ri0.y1 and their xi0.a params class) and the
     * location/transfer-adjacent classes already exercised by verified features are
     * confirmed either from decompiled method bodies or from an unambiguous structural
     * match; a handful of rarely-exercised ones (quoteStorage/voiceStoragePath/
     * videoBuildTask/emojiManager/locationBuildTask/locationSendTask method names, the
     * mini-program r45.* aliases, and the w11.s1/aq1.l fallback text-send builder) are
     * carried over unverified from 8.0.78 as placeholders, same as 8.0.78 was seeded
     * from 8.0.77 -- correct the first time the feature that uses one is exercised and
     * fails.
     */
    private static final Profile WECHAT_8079 = new Profile(
            "8.0.79",
            false,
            false,
            true,
            true,
            true,
            methods("Wi", "bk", "Bj", "Cj", "j2", "Ej", "zj", "Gj", "Ij", "Ij", "Mj", "Cj", "Gj", "Ij"),
            fields("x2", "i0", "k2", "j2", "l2", "C2", "t2", "B2", "m2"),
            aliases(
                    "i95.n0", "ni5.n0",
                    "i95.y", "ni5.y",
                    "k95.a", "pi5.a",
                    "w11.r0", "f61.s0",
                    "w11.s1", "xw1.p1",
                    "aq1.l", "z10.n1",
                    "c01.e2", "l41.d2",
                    "c01.f7", "l41.i7",
                    "c01.h7", "l41.k7",
                    "c01.ia", "e31.ba",
                    "aa0.e", "zb0.e",
                    "ui3.b", "zz.b",
                    "ot0.q", "nx0.r",
                    "tg3.u0", "no3.u0",
                    "bm5.y", "pv5.x",
                    "bm5.f0", "pv5.e0",
                    "oi3.g", "oq3.g",
                    "cg0.d", "fj0.d",
                    "dg0.f", "gj0.f",
                    "qi3.b0", "qq3.c0",
                    "qi3.x", "qq3.y",
                    "ut.s0", "gu.o0",
                    "dk5.w", "ud6.w",
                    "vf0.r2", "yi0.t2",
                    "vf0.u2", "yi0.w2",
                    "vf0.i3", "yi0.k3",
                    "vf0.h3", "yi0.j3",
                    "vf0.p2", "yi0.r2",
                    "wf0.b2", "zi0.b2",
                    "y80.q1", "sb0.t1",
                    "z80.h0", "tb0.h0",
                    "y80.p0", "sb0.p0",
                    "r45.uf6", "nd5.fr6",
                    "r45.vh4", "nd5.gq4",
                    "r45.xz6", "nd5.hc7",
                    "r15.b", "na5.d",
                    "r15.d", "na5.f",
                    "qh3.a1", "no3.c1",
                    "jg0.x", "nj0.x",
                    "h70.y", "ea0.y",
                    "h70.w", "ea0.w",
                    "qt0.k0", "ox0.k0",
                    "com.tencent.mm.vfs.w6", "com.tencent.mm.vfs.a7",
                    "com.tencent.mm.storage.f9", "com.tencent.mm.storage.e9",
                    "xi0.a", "fj0.a",
                    "ri0.y1", "zi0.y1",
                    // Same "short name under an otherwise-stable com.tencent.mm.* package
                    // gets reassigned to an unrelated class" pattern already hit by
                    // MMApplicationContext (x2 -> a3) in 8.0.78; the NetSceneQueue helper
                    // (readiness check "c"/enqueue "b") moved the same way here, confirmed
                    // structurally identical at com.tencent.mm.modelbase.a3.
                    "com.tencent.mm.modelbase.z2", "com.tencent.mm.modelbase.a3",
                    "com.tencent.mm.modelbase.m1", "com.tencent.mm.modelbase.n1",
                    // NetSceneQueue itself (implements com.tencent.mm.app.s2, the stable
                    // un-obfuscated onAppForeground/onAppBackground interface). Canonical
                    // baseline name is 8.0.78's real name (r1); 8.0.79 reassigned it to s1.
                    // Confirmed via decompiled source: both have identical structure (same
                    // "MicroMsg.NetSceneQueue" log tag, same onAppBackground/onAppForeground
                    // overrides) -- not a guess.
                    "com.tencent.mm.modelbase.r1", "com.tencent.mm.modelbase.s1"));

    /**
     * WeChat 8.0.80. Resolved via the same structural dex-shape matching used for 8.0.79
     * (full-class shape index of both versions, cross-checked with partial jadx decompile
     * for the handful of identical-signature "twin method" traps already known from 8.0.79
     * -- see the voiceStoragePath/voiceSendTask comments below). Confidence is noted inline:
     * SOLID/CONFIRMED entries were cross-verified two independent ways (e.g. an exact
     * constructor-signature match plus an independent structural top-candidate, or a
     * decompiled source read); entries marked weak/seeded are a single best-effort
     * structural guess carried through unverified, same as every prior version's first pass
     * -- expect to correct them the first time the feature that uses one is exercised and
     * fails, exactly like 8.0.76 through 8.0.79 were each refined in turn.
     */
    private static final Profile WECHAT_8080 = new Profile(
            "8.0.80",
            false,
            false,
            true,
            true,
            true,
            // voiceStoragePath="hk" and voiceSendTask="Sj" are CONFIRMED via decompiled
            // source read (not guessed): mc0.b has the same identical-signature twin-method
            // trap as 8.0.79's Ej/bk pair (here Kj/hk), and Kj is again the undecompilable
            // one ("Method dump skipped") while hk has the real "[f9]getVoiceFullPath" body.
            // wj0.o's voiceSendTask candidates (Nj/Oj/Sj, all void(task)) were disambiguated
            // by reading their bodies: Nj is "cancelTask", Oj is "initSync", Sj is the real
            // send entry point (delegates into the async Rj path). videoBuildTask="Oj" (on
            // the OTHER class, fj0.e3) is CONFIRMED via exact signature match: Oj(fj0.n3)
            // returns hr3.f0, exactly matching (h3Class)->qi3.b0. The rest of this row is
            // carried over from 8.0.79 unverified, same as every prior first pass.
            methods("Wi", "hk", "Bj", "Cj", "j2", "Ej", "zj", "Gj", "Ij", "Oj", "Sj", "Cj", "Gj", "Ij"),
            fields("x2", "i0", "k2", "j2", "l2", "C2", "t2", "B2", "m2"),
            aliases(
                    "i95.n0", "fi5.n0",
                    // Package-sibling inferred from i95.n0 (both under the same ni5->fi5
                    // whole-package rename); not independently verified.
                    "i95.y", "fi5.y",
                    // CONFIRMED via implementer trick: com.tencent.mm.app.l0 (unchanged real
                    // name) now implements hi5.a instead of pi5.a.
                    "k95.a", "hi5.a",
                    "w11.r0", "q61.s0",
                    "w11.s1", "xw1.p1",
                    "aq1.l", "g20.n1",
                    "c01.e2", "w41.e2",
                    // CONFIRMED independently twice: direct structural match AND as a
                    // positional hit inside the video constructor signature cross-check
                    // below (Lw41/j7; is the first param, matching old Ll41/i7;).
                    "c01.f7", "w41.j7",
                    "c01.h7", "w41.l7",
                    "c01.ia", "p31.fa",
                    // CONFIRMED via implementer trick: zm3.k0 (old) -> qn3.k0 (new),
                    // interfaces=['hc0.e'].
                    "aa0.e", "hc0.e",
                    "ui3.b", "zz.b",
                    "ot0.q", "xx0.r",
                    // CONFIRMED via implementer trick: ec0.b (old) -> mc0.b (new, same class
                    // that holds the voiceStoragePath twin-method trap above),
                    // interfaces=['ep3.u0'].
                    "tg3.u0", "ep3.u0",
                    "bm5.y", "jv5.x",
                    "bm5.f0", "jv5.e0",
                    "oi3.g", "oq3.g",
                    "cg0.d", "pj0.d",
                    // CONFIRMED via implementer trick: mj0.o (old) -> wj0.o (new, same class
                    // disambiguated for voiceSendTask above), interfaces=['qj0.f'].
                    "dg0.f", "qj0.f",
                    // Both CONFIRMED via method-signature read on the resolved dispatcher
                    // class itself: hr3.b0.d(Lhr3/f0;) matches the known
                    // "d(task)->Flow" dispatch shape, with hr3.f0 as the task-interface
                    // parameter type.
                    "qi3.b0", "hr3.f0",
                    "qi3.x", "hr3.b0",
                    "ut.s0", "lu.o0",
                    "dk5.w", "ud6.w",
                    // The entire vf0.* video-constructor-params family below is CONFIRMED
                    // via the exact constructor signature of vf0.r2's resolved class
                    // (fj0.x2), read directly from the shape index:
                    // (Lw41/j7; Lgd5/jo6; Ljava/lang/String; Lgd5/h97; Ljava/lang/String;
                    //  Lgd5/iq4; Z Lfj0/v2; Ljava/lang/String; Lga5/f; Z Z Z
                    //  Ljava/lang/String; ...)V
                    // cross-checked against vf0.h3's resolved class (fj0.n3) constructor
                    // (Ljava/lang/String; Lfj0/a3; Z Lfj0/o3;)V matching (String, u2, Z, i3)
                    // -- this is the same technique that nailed down r45.uf6/xz6 for 8.0.79.
                    "vf0.r2", "fj0.x2",
                    "vf0.u2", "fj0.a3",
                    "vf0.i3", "fj0.o3",
                    "vf0.h3", "fj0.n3",
                    "vf0.p2", "fj0.v2",
                    // CONFIRMED via implementer trick: yi0.a3 (old) -> fj0.e3 (new, the same
                    // class videoBuildTask="Oj" was read from above), interfaces=['gj0.a2'].
                    "wf0.b2", "gj0.a2",
                    "y80.q1", "ac0.u1",
                    // CONFIRMED via implementer trick: sb0.p0 (old) -> ac0.q0 (new, same
                    // sibling-package pattern as y80.q1 below it: t1->u1, p0->q0),
                    // interfaces=['bc0.h0'].
                    "z80.h0", "bc0.h0",
                    "y80.p0", "ac0.q0",
                    // CONFIRMED via the vf0.r2 constructor cross-check above (positions 2/4/6).
                    "r45.uf6", "gd5.jo6",
                    "r45.vh4", "gd5.iq4",
                    "r45.xz6", "gd5.h97",
                    "r15.b", "ga5.d",
                    // CONFIRMED via the vf0.r2 constructor cross-check above (position 10).
                    "r15.d", "ga5.f",
                    // CONFIRMED via implementer trick: rt5.i5 (old) -> lt5.d5 (new),
                    // interfaces=['ep3.c1'].
                    "qh3.a1", "ep3.c1",
                    "jg0.x", "xj0.x",
                    // Both CONFIRMED via positional interface-list comparison: old da0.e
                    // implements ['ea0.g0','ea0.v','ea0.y','ea0.z'] (whole ea0 package);
                    // new la0.e implements ['ma0.g0','ma0.v','ma0.y','ma0.z'] in the exact
                    // same order -- ea0.y (h70.y) -> ma0.y, and ea0.w (h70.w, a sibling of
                    // the same whole-package rename) -> ma0.w (confirmed to exist in the new
                    // index with a matching empty-interface shape).
                    "h70.y", "ma0.y",
                    "h70.w", "ma0.w",
                    "qt0.k0", "yx0.k0",
                    "com.tencent.mm.vfs.w6", "com.tencent.mm.vfs.a7",
                    "com.tencent.mm.storage.f9", "com.tencent.mm.storage.e9",
                    // CONFIRMED via decompiled source read: wj0.o.Mj(pj0.a params) is the
                    // asyncDownloadVoice entry point, taking this exact params class.
                    "xi0.a", "pj0.a",
                    "ri0.y1", "zi0.y1",
                    "com.tencent.mm.modelbase.z2", "com.tencent.mm.modelbase.a3",
                    "com.tencent.mm.modelbase.m1", "com.tencent.mm.modelbase.n1",
                    "com.tencent.mm.modelbase.r1", "com.tencent.mm.modelbase.s1"));

    private WeChatVersionSymbols() {
    }

    static Profile resolve(ClassProbe probe) {
        if (probe.isPresent(WECHAT_8080.className("i95.n0"))
                && probe.isPresent(WECHAT_8080.className("w11.r0"))) {
            return WECHAT_8080;
        }
        if (probe.isPresent(WECHAT_8079.className("i95.n0"))
                && probe.isPresent(WECHAT_8079.className("w11.r0"))) {
            return WECHAT_8079;
        }
        if (probe.isPresent(WECHAT_8078.className("i95.n0"))
                && probe.isPresent(WECHAT_8078.className("w11.r0"))) {
            return WECHAT_8078;
        }
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
