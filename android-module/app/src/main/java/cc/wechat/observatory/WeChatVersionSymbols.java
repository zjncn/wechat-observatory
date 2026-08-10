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
        private final Map<String, String> aliases;

        Profile(
                String version,
                boolean extensionRegistryRequired,
                boolean forceServiceInitialization,
                boolean featureServiceSendStack,
                Map<String, String> aliases) {
            this.version = version;
            this.extensionRegistryRequired = extensionRegistryRequired;
            this.forceServiceInitialization = forceServiceInitialization;
            this.featureServiceSendStack = featureServiceSendStack;
            this.aliases = Collections.unmodifiableMap(new HashMap<>(aliases));
        }

        String className(String baselineName) {
            String alias = aliases.get(baselineName);
            return alias == null ? baselineName : alias;
        }
    }

    private static final Profile WECHAT_8075 = new Profile(
            "8.0.75",
            true,
            true,
            false,
            Collections.<String, String>emptyMap());

    private static final Profile WECHAT_8076 = new Profile(
            "8.0.76",
            false,
            false,
            true,
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

    private WeChatVersionSymbols() {
    }

    static Profile resolve(ClassProbe probe) {
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
