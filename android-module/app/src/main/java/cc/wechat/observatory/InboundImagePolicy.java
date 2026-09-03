/*
 * @input WeChat image path hints and image2 filenames
 * @output Original-image candidate names with thumbnail files excluded
 * @position Pure inbound-image selection policy shared by HookEntry and unit tests
 * @auto-doc Update folder INDEX.md when this file changes
 */
package cc.wechat.observatory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

final class InboundImagePolicy {
    private static final String[] THUMBNAIL_PREFIXES = new String[]{"msgth_", "thumb_", "th_"};

    private InboundImagePolicy() {
    }

    static boolean isThumbnailPath(String path) {
        if (path == null || path.trim().isEmpty()) {
            return false;
        }
        String normalized = path.trim().replace('\\', '/');
        int slash = normalized.lastIndexOf('/');
        String name = slash >= 0 ? normalized.substring(slash + 1) : normalized;
        String lower = name.toLowerCase(Locale.US);
        for (String prefix : THUMBNAIL_PREFIXES) {
            if (lower.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    static List<String> originalCandidateNames(String mediaHint) {
        List<String> names = new ArrayList<>();
        if (mediaHint == null || mediaHint.trim().isEmpty()) {
            return names;
        }
        String normalized = mediaHint.trim();
        int query = normalized.indexOf('?');
        if (query >= 0) {
            normalized = normalized.substring(0, query);
        }
        int scheme = normalized.indexOf("://");
        if (scheme >= 0 && scheme + 3 < normalized.length()) {
            normalized = normalized.substring(scheme + 3);
        }
        normalized = normalized.trim();
        addOriginalCandidate(names, normalized);
        int slash = Math.max(normalized.lastIndexOf('/'), normalized.lastIndexOf(File.separatorChar));
        if (slash >= 0 && slash + 1 < normalized.length()) {
            addOriginalCandidate(names, normalized.substring(slash + 1));
        }
        return names;
    }

    private static void addOriginalCandidate(List<String> names, String candidate) {
        if (candidate == null || candidate.trim().isEmpty()) {
            return;
        }
        String value = candidate.trim();
        String fileName = value.replace('\\', '/');
        int slash = fileName.lastIndexOf('/');
        fileName = slash >= 0 ? fileName.substring(slash + 1) : fileName;
        String lower = fileName.toLowerCase(Locale.US);
        for (String prefix : THUMBNAIL_PREFIXES) {
            if (lower.startsWith(prefix) && fileName.length() > prefix.length()) {
                int nameOffset = value.length() - fileName.length();
                addUnique(names, value.substring(0, nameOffset) + fileName.substring(prefix.length()));
                return;
            }
        }
        addUnique(names, value);
    }

    private static void addUnique(List<String> names, String value) {
        if (value == null || value.trim().isEmpty()) {
            return;
        }
        for (String existing : names) {
            if (existing.equals(value)) {
                return;
            }
        }
        names.add(value);
    }
}
