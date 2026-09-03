package cc.wechat.observatory.util;

import java.util.Locale;

public final class Strings {
    private Strings() {
    }

    public static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    public static String trimRight(String value, String suffix) {
        String out = value == null ? "" : value;
        while (out.endsWith(suffix)) {
            out = out.substring(0, out.length() - suffix.length());
        }
        return out;
    }

    public static String json(String value) {
        if (value == null) {
            return "";
        }
        StringBuilder out = new StringBuilder(value.length() + 16);
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '"':
                    out.append("\\\"");
                    break;
                case '\\':
                    out.append("\\\\");
                    break;
                case '\b':
                    out.append("\\b");
                    break;
                case '\f':
                    out.append("\\f");
                    break;
                case '\n':
                    out.append("\\n");
                    break;
                case '\r':
                    out.append("\\r");
                    break;
                case '\t':
                    out.append("\\t");
                    break;
                default:
                    if (ch < 0x20) {
                        out.append(String.format(Locale.US, "\\u%04x", (int) ch));
                    } else {
                        out.append(ch);
                    }
            }
        }
        return out.toString();
    }

    public static String shortError(Throwable throwable) {
        if (throwable == null) {
            return "";
        }
        String message = throwable.getMessage();
        if (isBlank(message)) {
            message = throwable.toString();
        }
        return message.length() > 180 ? message.substring(0, 180) + "..." : message;
    }

    /**
     * Unlike {@link #shortError}, walks the full cause chain. ART wraps a class's
     * original init/verify failure inside later NoClassDefFoundError/VerifyError
     * throws for that same class; the JVM/ART spec requires the original
     * exception to be attached as the cause, but {@link #shortError} only reads
     * getMessage() on the outermost throwable and drops it.
     */
    public static String fullErrorChain(Throwable throwable) {
        if (throwable == null) {
            return "";
        }
        StringBuilder out = new StringBuilder();
        Throwable current = throwable;
        Throwable deepest = throwable;
        int depth = 0;
        while (current != null && depth < 8) {
            if (depth > 0) {
                out.append(" <- caused by ");
            }
            out.append(current.getClass().getName());
            String message = current.getMessage();
            if (!isBlank(message)) {
                out.append(": ").append(message.length() > 300 ? message.substring(0, 300) + "..." : message);
            }
            deepest = current;
            Throwable cause = current.getCause();
            if (cause == current) {
                break;
            }
            current = cause;
            depth++;
        }
        out.append(" | ").append(deepest.getClass().getSimpleName()).append(" stack: ");
        StackTraceElement[] frames = deepest.getStackTrace();
        int frameCount = Math.min(frames == null ? 0 : frames.length, 6);
        for (int i = 0; i < frameCount; i++) {
            if (i > 0) {
                out.append(" / ");
            }
            out.append(frames[i]);
        }
        return out.toString();
    }
}
