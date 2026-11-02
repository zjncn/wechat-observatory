/*
 * @input LSPosed/Xposed hook callbacks, BridgeConfig, WeChat runtime classes, HTTP/WebSocket transport
 * @output Message observation uploads, contact snapshots, Action Outbox sends, and source-bound transfer acceptance/decline
 * @position Central Android module runtime that bridges WeChat internals with the gateway service
 * @auto-doc Update header and folder INDEX.md when this file changes
 */
package cc.wechat.observatory;

import android.content.ContentValues;
import android.content.Context;
import android.content.ContextWrapper;
import android.app.Application;
import android.os.Handler;
import android.os.Looper;
import android.os.Parcel;
import android.util.Base64;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import cc.wechat.observatory.config.BridgeConfig;
import cc.wechat.observatory.gateway.WebSocketFrame;
import cc.wechat.observatory.model.MessagePayload;
import cc.wechat.observatory.util.BridgeLogger;
import cc.wechat.observatory.wechat.SendResult;
import de.robv.android.xposed.IXposedHookLoadPackage;
import de.robv.android.xposed.XC_MethodHook;
import de.robv.android.xposed.XposedBridge;
import de.robv.android.xposed.XposedHelpers;
import de.robv.android.xposed.callbacks.XC_LoadPackage;

import static cc.wechat.observatory.util.Strings.fullErrorChain;
import static cc.wechat.observatory.util.Strings.isBlank;
import static cc.wechat.observatory.util.Strings.json;
import static cc.wechat.observatory.util.Strings.shortError;
import static cc.wechat.observatory.util.Strings.trimRight;

public final class HookEntry implements IXposedHookLoadPackage {
    private static final String WECHAT_PACKAGE = "com.tencent.mm";
    private static final int MESSAGE_TYPE_QUOTE = 822083633;
    private static final int MESSAGE_TYPE_APPMSG = 49;
    private static final int MESSAGE_TYPE_FILE_TRANSFER = 1090519089;
    private static final int MESSAGE_TYPE_TRANSFER = 419430449;
    private static final int MESSAGE_TYPE_RED_PACKET = 436207665;
    private static final int APPMSG_TYPE_LINK = 5;
    private static final int APPMSG_TYPE_FILE = 6;
    private static final int APPMSG_TYPE_CHAT_HISTORY = 19;
    private static final int APPMSG_TYPE_MINI_PROGRAM = 33;
    private static final int APPMSG_TYPE_MINI_PROGRAM_LEGACY = 36;
    private static final int APPMSG_TYPE_QUOTE = 57;
    private static final int MAX_CHAT_HISTORY_SOURCE_ITEMS = 50;
    private static final int TRANSFER_STARTUP_BACKFILL_LIMIT = 200;
    private static final long TRANSFER_STARTUP_BACKFILL_WINDOW_SECONDS = 24L * 60L * 60L;
    private static final long OUTBOX_MEDIA_DOWNLOAD_LIMIT_BYTES = 50L * 1024L * 1024L;
    private static final AtomicBoolean WORKER_STARTED = new AtomicBoolean(false);
    private static final AtomicBoolean OUTBOX_WORKER_STARTED = new AtomicBoolean(false);
    private static final AtomicBoolean EXTERNAL_STORAGE_FALLBACK_HOOKED = new AtomicBoolean(false);
    private static final AtomicBoolean TRANSFER_CONFIRM_CALLBACK_HOOKED = new AtomicBoolean(false);
    private static final AtomicBoolean TRANSFER_QUERY_CALLBACK_HOOKED = new AtomicBoolean(false);
    private static final AtomicBoolean WALLET_PLUGIN_INITIALIZED = new AtomicBoolean(false);
    private static final AtomicBoolean WALLET_CLASSLOADERS_LOGGED = new AtomicBoolean(false);
    private static final AtomicBoolean TENPAY_SIGN_FALLBACK_HOOKED = new AtomicBoolean(false);
    private static final AtomicBoolean TENPAY_SIGN_FALLBACK_USED = new AtomicBoolean(false);
    private static final AtomicBoolean TENPAY_SIGN_FALLBACK_TRANSFER_USED = new AtomicBoolean(false);
    private static final AtomicBoolean TENPAY_SIGN_FALLBACK_SELF_TESTED = new AtomicBoolean(false);
    private static final AtomicBoolean TENPAY_TRANSFER_SCENE_SELF_TESTED = new AtomicBoolean(false);
    private static final AtomicBoolean TENPAY_CALLERS_DEOPTIMIZED = new AtomicBoolean(false);
    private static final ThreadLocal<Boolean> TENPAY_SIGN_FALLBACK_ACTIVE = new ThreadLocal<>();
    private static final ThreadLocal<Boolean> TENPAY_SIGN_FALLBACK_TRANSFER_ACTIVE = new ThreadLocal<>();
    private static final ConcurrentHashMap<Object, NetworkResultWaiter> TRANSFER_CONFIRM_WAITERS = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Object, NetworkResultWaiter> TRANSFER_QUERY_WAITERS = new ConcurrentHashMap<>();
    private static final ExecutorService POST_EXECUTOR = Executors.newFixedThreadPool(4, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "wechat-observatory-post");
            thread.setDaemon(true);
            return thread;
        }
    });
    private static volatile String LAST_READY_STATE = "";
    private static volatile String LAST_CLASSLOADER_STATE = "";
    private static volatile Object LAST_DATABASE;
    private static volatile long LAST_CONTACT_SYNC_AT = 0L;
    private static volatile long LAST_CONTACT_SYNC_SKIP_LOG_AT = 0L;
    private static volatile long LAST_CONTACT_SCAN_LOG_AT = 0L;
    private static volatile long LAST_MESSAGE_POLL_AT = 0L;
    private static volatile long LAST_MESSAGE_ID = 0L;
    private static volatile boolean TRANSFER_STARTUP_BACKFILL_DONE = false;
    private static volatile boolean MESSAGE_WATERMARK_READY = false;
    private static volatile int MESSAGE_POLL_CONSECUTIVE_FAILS = 0;
    private static volatile long MESSAGE_POLL_BACKOFF_UNTIL = 0L;
    private static volatile long LAST_MESSAGE_POLL_FAIL_LOG_AT = 0L;
    private static final long MESSAGE_POLL_BACKOFF_MS = 5000L;
    private static final long[] MEDIA_RETRY_DELAYS_MS = new long[]{3000L, 10000L, 30000L, 120000L};
    private static final Set<Long> MEDIA_RETRY_SCHEDULED_IDS = new HashSet<>();
    private static final Set<Long> MEDIA_UPLOAD_REPORTED_IDS = new HashSet<>();
    private static final Set<Long> ORIGINAL_IMAGE_DOWNLOAD_DISPATCHED_IDS = new HashSet<>();
    private static final Set<Long> VOICE_DOWNLOAD_DISPATCHED_IDS = new HashSet<>();
    private static final Set<Long> VIDEO_DOWNLOAD_DISPATCHED_IDS = new HashSet<>();
    private static final Map<Long, String> ORIGINAL_IMAGE_DOWNLOADED_PATHS = new LinkedHashMap<>();
    private static final Set<Long> ORIGINAL_IMAGE_DIAGNOSTIC_REPORTED_IDS = new HashSet<>();
    private static final Set<String> EMOJI_INFO_DIAGNOSTIC_REPORTED_MD5 = new HashSet<>();
    private static volatile long LAST_MEDIA_RETRY_FAIL_LOG_AT = 0L;
    private static volatile long LAST_WEBSOCKET_FAIL_LOG_AT = 0L;
    private static volatile String CURRENT_WXID = "";
    private static volatile String CURRENT_NICKNAME = "";
    private static volatile String REGISTERED_KEY = "";
    private static volatile String REGISTERED_DEVICE = "";
    private static volatile long LAST_REGISTER_ATTEMPT_AT = 0L;
    private static volatile long LAST_REGISTER_SUCCESS_AT = 0L;
    private static volatile long LAST_USER_SKIP_LOG_AT = 0L;
    private static volatile Context APP_CONTEXT;
    private static volatile ClassLoader WECHAT_CLASS_LOADER;
    private static volatile WeChatVersionSymbols.Profile WECHAT_SYMBOLS;
    private static volatile ClassLoader WECHAT_SYMBOLS_CLASS_LOADER;

    private static final class QuoteSource {
        long msgId;
        long msgSvrId;
        String talker;
        String content;
        int isSend;
        long createTime;
        int type;
        String msgSource;
        String senderWxid;
    }

    private static final class RevokeSource {
        long msgId;
        long msgSvrId;
        String talker;
        int isSend;
        long createTime;
        int type;
    }

    private static final class ChatHistorySource {
        long msgId;
        long msgSvrId;
        String talker;
        String content;
        int isSend;
        long createTime;
        int type;
        String msgSource;
    }

    private static final class ImageInfoRecord {
        long id;
        long hdId;
        String bigPath = "";
        String midPath = "";
        int totalLen;
        int offset;

        boolean isComplete() {
            return totalLen > 0 && offset >= totalLen;
        }
    }

    private static final class OutboxDispatchItem {
        final int index;
        final JSONObject item;
        final long id;
        final String wxid;
        final String kind;
        final String laneKey;

        OutboxDispatchItem(int index, JSONObject item, long id, String wxid, String kind) {
            this.index = index;
            this.item = item;
            this.id = id;
            this.wxid = wxid == null ? "" : wxid.trim();
            this.kind = kind == null ? "text" : kind;
            this.laneKey = this.wxid + "|" + this.kind;
        }
    }

    private static final class OutboxAckEntry {
        final int index;
        final JSONObject ack;

        OutboxAckEntry(int index, JSONObject ack) {
            this.index = index;
            this.ack = ack;
        }
    }

    @Override
    public void handleLoadPackage(XC_LoadPackage.LoadPackageParam lpparam) {
        if (!WECHAT_PACKAGE.equals(lpparam.packageName)) {
            return;
        }
        BridgeConfig config = BridgeConfig.load(null);
        if (!isTargetAndroidUser(config)) {
            logTargetUserSkip("skip WeChat hook in process " + lpparam.processName);
            return;
        }
        log("loading build video-send-feature-db-v2 process=" + lpparam.processName);
        hookExternalStorageFallback();
        boolean mainProcess = WECHAT_PACKAGE.equals(lpparam.processName);

        try {
            Class<?> sqliteDatabase = XposedHelpers.findClass("com.tencent.wcdb.database.SQLiteDatabase", lpparam.classLoader);
            XposedHelpers.findAndHookMethod(
                    sqliteDatabase,
                    "insertWithOnConflict",
                    String.class,
                    String.class,
                    ContentValues.class,
                    int.class,
                    new XC_MethodHook() {
                        @Override
                        protected void afterHookedMethod(MethodHookParam param) {
                            handleInsert(param);
                        }
                    });
            hookDatabaseCaptureMethods(sqliteDatabase);
            log("hooked WeChat WCDB access methods");
            if (mainProcess) {
                hookApplicationAttach(lpparam.classLoader);
                hookWeChatApplication(lpparam.classLoader);
                hookVoiceDownloadFinishDiagnostic(lpparam.classLoader);
            } else {
                log("skip outbox worker in process " + lpparam.processName);
            }
        } catch (Throwable t) {
            log("hook failed: " + t);
        }
    }

    private static void hookExternalStorageFallback() {
        if (!EXTERNAL_STORAGE_FALLBACK_HOOKED.compareAndSet(false, true)) {
            return;
        }
        try {
            XposedHelpers.findAndHookMethod(
                    ContextWrapper.class,
                    "getExternalCacheDir",
                    new XC_MethodHook() {
                        @Override
                        protected void beforeHookedMethod(MethodHookParam param) {
                            Context context = contextFromWrapper(param.thisObject);
                            File dir = fallbackExternalDir(context, true, null);
                            if (dir != null) {
                                param.setResult(dir);
                            }
                        }
                    });
            XposedHelpers.findAndHookMethod(
                    ContextWrapper.class,
                    "getExternalCacheDirs",
                    new XC_MethodHook() {
                        @Override
                        protected void beforeHookedMethod(MethodHookParam param) {
                            Context context = contextFromWrapper(param.thisObject);
                            File dir = fallbackExternalDir(context, true, null);
                            if (dir != null) {
                                param.setResult(new File[]{dir});
                            }
                        }
                    });
            XposedHelpers.findAndHookMethod(
                    ContextWrapper.class,
                    "getExternalFilesDir",
                    String.class,
                    new XC_MethodHook() {
                        @Override
                        protected void beforeHookedMethod(MethodHookParam param) {
                            Context context = contextFromWrapper(param.thisObject);
                            String type = param.args != null && param.args.length > 0 && param.args[0] instanceof String
                                    ? (String) param.args[0]
                                    : null;
                            File dir = fallbackExternalDir(context, false, type);
                            if (dir != null) {
                                param.setResult(dir);
                            }
                        }
                    });
            XposedHelpers.findAndHookMethod(
                    ContextWrapper.class,
                    "getExternalFilesDirs",
                    String.class,
                    new XC_MethodHook() {
                        @Override
                        protected void beforeHookedMethod(MethodHookParam param) {
                            Context context = contextFromWrapper(param.thisObject);
                            String type = param.args != null && param.args.length > 0 && param.args[0] instanceof String
                                    ? (String) param.args[0]
                                    : null;
                            File dir = fallbackExternalDir(context, false, type);
                            if (dir != null) {
                                param.setResult(new File[]{dir});
                            }
                        }
                    });
            log("hooked external storage fallback");
        } catch (Throwable t) {
            log("hook external storage fallback failed: " + shortError(t));
        }
    }

    // Diagnostic only: WeChat's own success/failure logging for voice downloads
    // (tag "MicroMsg.VoiceDownloadTask") goes through its internal mars/xlog system, not
    // logcat, so there is otherwise no visibility into whether a download we triggered via
    // requestVoiceDownloadAsync ever reaches a terminal state. Hooking the generic event
    // publish method and filtering to this one event type surfaces that outcome directly.
    private static void hookVoiceDownloadFinishDiagnostic(ClassLoader classLoader) {
        try {
            Class<?> eventClass = XposedHelpers.findClass("com.tencent.mm.sdk.event.IEvent", classLoader);
            final Class<?> targetClass = XposedHelpers.findClass(
                    "com.tencent.mm.autogen.events.VoiceMsgDownloadFinishEvent", classLoader);
            XposedHelpers.findAndHookMethod(eventClass, "b", Looper.class, new XC_MethodHook() {
                @Override
                protected void beforeHookedMethod(MethodHookParam param) {
                    if (!targetClass.isInstance(param.thisObject)) {
                        return;
                    }
                    try {
                        // jadx displays these as f60594g/f275754a/f275755b/f275756c to avoid a
                        // name collision in the decompiled source, but says so right in its own
                        // "renamed from" comment -- the real reflective names are the short ones.
                        Object payload = findField(param.thisObject.getClass(), "g").get(param.thisObject);
                        long msgId = ((Number) findField(payload.getClass(), "a").get(payload)).longValue();
                        boolean success = ((Boolean) findField(payload.getClass(), "b").get(payload)).booleanValue();
                        boolean redownload = ((Boolean) findField(payload.getClass(), "c").get(payload)).booleanValue();
                        log("voice download finished msgId=" + msgId + " success=" + success + " isRedownload=" + redownload);
                    } catch (Throwable t) {
                        log("voice download finish diagnostic failed: " + shortError(rootCause(t)));
                    }
                }
            });
            log("hooked VoiceMsgDownloadFinishEvent diagnostic");
        } catch (Throwable t) {
            log("hook voice download diagnostic failed: " + shortError(rootCause(t)));
        }
    }

    // Diagnostic only: confirms (a) the reflective onAppForeground call in
    // wakeNetSceneQueueForeground actually reaches the real method body rather than silently
    // no-op'ing on a wrong class/overload, and (b) what the "d"/"h" enqueue-path methods
    // actually do to the queue's internal state on a real send -- WeChat's own logging for
    // this class goes through com.tencent.mars.xlog.Log, which writes to its own log files,
    // not logcat, so there is otherwise no visibility into this from outside the process.
    private static Context contextFromWrapper(Object value) {
        if (value instanceof Context) {
            return (Context) value;
        }
        Context context = APP_CONTEXT;
        if (context != null) {
            return context;
        }
        Object app = currentApplication();
        return app instanceof Context ? (Context) app : null;
    }

    private static File fallbackExternalDir(Context context, boolean cache, String type) {
        if (context == null) {
            return null;
        }
        File root = cache ? context.getCacheDir() : context.getFilesDir();
        if (root == null) {
            return null;
        }
        File dir = new File(root, cache ? "external_cache" : "external_files");
        if (!cache && !isBlank(type)) {
            dir = new File(dir, safePathSegment(type));
        }
        if (!dir.isDirectory() && !dir.mkdirs()) {
            return null;
        }
        return dir;
    }

    private static String safePathSegment(String value) {
        if (isBlank(value)) {
            return "";
        }
        return value.replace('\\', '_').replace('/', '_').trim();
    }

    private static void hookDatabaseCaptureMethods(Class<?> sqliteDatabase) {
        for (Method method : sqliteDatabase.getDeclaredMethods()) {
            String name = method.getName();
            if (!shouldHookDatabaseMethod(name) || Modifier.isStatic(method.getModifiers())) {
                continue;
            }
            try {
                method.setAccessible(true);
                XposedBridge.hookMethod(method, new XC_MethodHook() {
                    @Override
                    protected void beforeHookedMethod(MethodHookParam param) {
                        captureDatabaseFromArgs(param.thisObject, param.args);
                    }

                    @Override
                    protected void afterHookedMethod(MethodHookParam param) {
                        captureDatabaseFromArgs(param.thisObject, param.args);
                    }
                });
            } catch (Throwable t) {
                log("hook db method " + name + " failed: " + shortError(t));
            }
        }
    }

    private static boolean shouldHookDatabaseMethod(String name) {
        return "rawQuery".equals(name)
                || "rawQueryWithFactory".equals(name)
                || "query".equals(name)
                || "queryWithFactory".equals(name)
                || "insert".equals(name)
                || "insertOrThrow".equals(name)
                || "replace".equals(name)
                || "replaceOrThrow".equals(name)
                || "update".equals(name)
                || "updateWithOnConflict".equals(name)
                || "delete".equals(name);
    }

    private static void captureDatabaseFromArgs(Object db, Object[] args) {
        if (db == null || args == null) {
            return;
        }
        for (Object arg : args) {
            if (arg instanceof String && looksLikeMessageTableAccess((String) arg)) {
                if (LAST_DATABASE != db) {
                    LAST_DATABASE = db;
                    log("captured WeChat database from " + shorten((String) arg, 80));
                }
                return;
            }
        }
    }

    private static boolean looksLikeMessageTableAccess(String value) {
        if (isBlank(value)) {
            return false;
        }
        String normalized = value.trim().toLowerCase(Locale.US);
        String padded = normalized
                .replace('\n', ' ')
                .replace('\r', ' ')
                .replace('\t', ' ');
        padded = " " + padded + " ";
        return "message".equals(normalized)
                || padded.contains(" from message ")
                || padded.contains(" join message ")
                || padded.contains(" update message ")
                || padded.contains(" into message ")
                || padded.contains(" delete from message ");
    }

    private static String shorten(String value, int maxLength) {
        if (value == null || value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength) + "...";
    }

    /**
     * Sets com.tencent.mm.sdk.platformtools.x2.a (WeChat's own MMApplicationContext
     * holder) as early as physically possible in the process lifecycle, if it isn't
     * set already. Root cause per handoff doc 13.26/13.27: on some devices WeChat's
     * own code doesn't set this field before something (WeChat itself, or one of our
     * other hooks) first touches com.tencent.mm.wallet_core.tenpay.model.o, whose
     * static init reads x2.a and NPEs if it's null — permanently marking o (and
     * transfer's n0/h1) erroneous for the rest of the process, no matter how the
     * field gets fixed afterward. Calling this from confirmTransfer/queryTransferStatus
     * alone was too late (13.27 retest still failed); wiring it into every
     * Application.attach/onBaseContextAttached/onCreate hook callback instead gives
     * it the earliest possible chance, before any wallet class gets touched. Cheap
     * and idempotent (a single field read) once x2.a is already set.
     */
    private static void ensureAppContextField(Context context, ClassLoader classLoader) {
        if (context == null || classLoader == null) {
            return;
        }
        try {
            // WeChat 8.0.78 reassigned "x2" to an unrelated class and renamed the
            // real MMApplicationContext holder to "a3" (setter method "u" -> "w").
            Class<?> appContextClass = findClassWithField(
                    classLoader,
                    new String[]{"com.tencent.mm.sdk.platformtools.x2", "com.tencent.mm.sdk.platformtools.a3"},
                    "f210311a", "a");
            Field field = findFieldAny(appContextClass, "f210311a", "a");
            if (field.get(null) == null) {
                Method setter = findMethodAny(appContextClass, new Class<?>[]{Context.class}, "u", "w");
                setter.invoke(null, context);
                log("initialized WeChat MMApplicationContext early from lifecycle hook");
            }
        } catch (Throwable t) {
            log("early MMApplicationContext init failed: " + shortError(t));
        }
    }

    /**
     * Initializes WeChat's generated build metadata before wallet NetScenes are
     * constructed. WeChat 8.0.77's request builder decodes
     * com.tencent.mm.sdk.platformtools.z.g as an integer; on the Android 16 cloned
     * profile this field can still be null when the bridge worker handles its first
     * transfer command. Use WeChat's own SetupBaseBuildInfo entry point instead of
     * copying a version constant into one field, so all dependent metadata stays
     * consistent across supported builds.
     */
    private static void ensureWeChatBuildInfo(Context context, ClassLoader classLoader) {
        if (context == null || classLoader == null) {
            return;
        }
        try {
            Class<?> buildInfoClass = findClass(classLoader, "com.tencent.mm.sdk.platformtools.z");
            Field clientVersionField = findFieldAny(buildInfoClass, "f198356g", "g");
            Object currentValue = clientVersionField.get(null);
            if (isDecodableInteger(currentValue)) {
                return;
            }

            // WeChat 8.0.78 reassigned "u5" to an unrelated enum and renamed the
            // real SetupBaseBuildInfo entry point to "b6" (method name "a" unchanged).
            Class<?> setupClass = findClassWithMethod(
                    classLoader,
                    new String[]{"com.tencent.mm.app.u5", "com.tencent.mm.app.b6"},
                    "a", Context.class);
            findMethod(setupClass, "a", Context.class).invoke(null, context);
            Object initializedValue = clientVersionField.get(null);
            if (!isDecodableInteger(initializedValue)) {
                throw new IllegalStateException("WeChat client version is still unavailable after SetupBaseBuildInfo");
            }
            log("initialized WeChat build info early from lifecycle hook");
        } catch (Throwable t) {
            log("early WeChat build info init failed: " + fullErrorChain(t));
        }
    }

    private static boolean isDecodableInteger(Object value) {
        if (!(value instanceof String) || isBlank((String) value)) {
            return false;
        }
        try {
            Integer.decode((String) value);
            return true;
        } catch (NumberFormatException ignored) {
            return false;
        }
    }

    /**
     * Activates WeChat's wallet feature through its own service manager. The wallet
     * service's onCreate callback loads libtenpay_utils and performs the other wallet
     * registrations as one lifecycle operation. This keeps ART's JNI association and
     * WeChat's internal loaded-library state consistent.
     */
    private static void ensureWalletPluginInitialized(ClassLoader classLoader) throws Exception {
        if (WALLET_PLUGIN_INITIALIZED.get()) {
            return;
        }
        synchronized (WALLET_PLUGIN_INITIALIZED) {
            if (WALLET_PLUGIN_INITIALIZED.get()) {
                return;
            }
            Class<?> serviceManagerClass = findVersionedClass(classLoader, "i95.n0");
            Class<?> walletPluginClass = findClass(classLoader, "ni0.j2");
            Object walletPlugin = findMethod(serviceManagerClass, "c", Class.class)
                    .invoke(null, walletPluginClass);
            if (walletPlugin == null) {
                throw new IllegalStateException("WeChat wallet feature service is unavailable");
            }
            WALLET_PLUGIN_INITIALIZED.set(true);
            log("initialized WeChat wallet feature service=" + walletPlugin.getClass().getName());
        }
    }

    /**
     * Android 16 can map libtenpay_utils successfully yet leave Encrypt.encrypt
     * unresolved in a cloned profile. Install a narrowly-scoped Java equivalent of
     * TenpayUtil.signWith3Des and activate it only while this module constructs a
     * transfer NetScene. Normal WeChat wallet calls are untouched.
     */
    private static void ensureTenpaySignFallbackHook(ClassLoader classLoader) throws Exception {
        if (TENPAY_SIGN_FALLBACK_HOOKED.get()) {
            return;
        }
        synchronized (TENPAY_SIGN_FALLBACK_HOOKED) {
            if (TENPAY_SIGN_FALLBACK_HOOKED.get()) {
                return;
            }
            XposedHelpers.findAndHookMethod(
                    "com.tencent.mm.wallet_core.tenpay.model.o",
                    classLoader,
                    "getEncryptUrl",
                    String.class,
                    new XC_MethodHook() {
                        @Override
                        protected void beforeHookedMethod(MethodHookParam param) throws Throwable {
                            if (!Boolean.TRUE.equals(TENPAY_SIGN_FALLBACK_ACTIVE.get())) {
                                return;
                            }
                            String source = param.args == null || param.args.length == 0
                                    ? null
                                    : (String) param.args[0];
                            param.setResult(TenpaySignFallback.sign(source));
                            logTenpayTransferFallback("NetSceneTenPayBase.getEncryptUrl");
                        }
                    });
            XposedHelpers.findAndHookMethod(
                    "com.tenpay.android.wechat.TenpayUtil",
                    classLoader,
                    "signWith3Des",
                    String.class,
                    new XC_MethodHook() {
                        @Override
                        protected void beforeHookedMethod(MethodHookParam param) throws Throwable {
                            if (!Boolean.TRUE.equals(TENPAY_SIGN_FALLBACK_ACTIVE.get())) {
                                return;
                            }
                            String source = param.args == null || param.args.length == 0
                                    ? null
                                    : (String) param.args[0];
                            param.setResult(TenpaySignFallback.sign(source));
                            if (TENPAY_SIGN_FALLBACK_USED.compareAndSet(false, true)) {
                                log("using scoped Java Tenpay sign fallback at TenpayUtil");
                            }
                            logTenpayTransferFallback("TenpayUtil.signWith3Des");
                        }
                    });
            XposedHelpers.findAndHookMethod(
                    "com.tenpay.ndk.Encrypt",
                    classLoader,
                    "desedeEncode",
                    String.class,
                    new XC_MethodHook() {
                        @Override
                        protected void beforeHookedMethod(MethodHookParam param) throws Throwable {
                            if (!Boolean.TRUE.equals(TENPAY_SIGN_FALLBACK_ACTIVE.get())) {
                                return;
                            }
                            String raw = param.args == null || param.args.length == 0
                                    ? null
                                    : (String) param.args[0];
                            param.setResult(TenpaySignFallback.encryptRaw(raw));
                            if (TENPAY_SIGN_FALLBACK_USED.compareAndSet(false, true)) {
                                log("using scoped Java Tenpay sign fallback at Encrypt.desedeEncode");
                            }
                            logTenpayTransferFallback("Encrypt.desedeEncode");
                        }
                    });
            XposedHelpers.findAndHookMethod(
                    "com.tenpay.ndk.Encrypt",
                    classLoader,
                    "encrypt",
                    byte[].class,
                    byte[].class,
                    new XC_MethodHook() {
                        @Override
                        protected void beforeHookedMethod(MethodHookParam param) throws Throwable {
                            if (!Boolean.TRUE.equals(TENPAY_SIGN_FALLBACK_ACTIVE.get())) {
                                return;
                            }
                            byte[] key = param.args == null ? null : (byte[]) param.args[0];
                            byte[] raw = param.args == null ? null : (byte[]) param.args[1];
                            findField(param.thisObject.getClass(), "enc_buf")
                                    .set(param.thisObject, TenpaySignFallback.encryptRaw(raw, key));
                            param.setResult(Boolean.TRUE);
                            if (TENPAY_SIGN_FALLBACK_USED.compareAndSet(false, true)) {
                                log("using scoped Java Tenpay sign fallback at Encrypt.encrypt");
                            }
                            logTenpayTransferFallback("Encrypt.encrypt");
                        }
                    });
            TENPAY_SIGN_FALLBACK_HOOKED.set(true);
            log("hooked scoped Tenpay sign fallback");
        }
    }

    private static Object newTransferScene(Constructor<?> constructor, Object... args) throws Exception {
        Boolean previous = TENPAY_SIGN_FALLBACK_ACTIVE.get();
        Boolean previousTransfer = TENPAY_SIGN_FALLBACK_TRANSFER_ACTIVE.get();
        TENPAY_SIGN_FALLBACK_ACTIVE.set(Boolean.TRUE);
        TENPAY_SIGN_FALLBACK_TRANSFER_ACTIVE.set(Boolean.TRUE);
        try {
            return newTransferSceneWithJavaSignature(constructor, args);
        } finally {
            if (previous == null) {
                TENPAY_SIGN_FALLBACK_ACTIVE.remove();
            } else {
                TENPAY_SIGN_FALLBACK_ACTIVE.set(previous);
            }
            if (previousTransfer == null) {
                TENPAY_SIGN_FALLBACK_TRANSFER_ACTIVE.remove();
            } else {
                TENPAY_SIGN_FALLBACK_TRANSFER_ACTIVE.set(previousTransfer);
            }
        }
    }

    /**
     * WeChat 8.0.77 calls Tenpay's unresolved JNI entry directly from compiled
     * constructor code on Android 16 cloned profiles, bypassing Java hooks even
     * after deoptimization. Allocate the two known NetScene types without running
     * their constructors, then reproduce those constructors and z0.setRequestData
     * exactly while supplying the already-tested Java WCPaySign implementation.
     */
    private static Object newTransferSceneWithJavaSignature(
            Constructor<?> constructor,
            Object[] args) throws Exception {
        Class<?> sceneClass = constructor.getDeclaringClass();
        String className = sceneClass.getName();
        if (!"com.tencent.mm.plugin.remittance.model.h1".equals(className)
                && !"com.tencent.mm.plugin.remittance.model.n0".equals(className)) {
            return constructor.newInstance(args);
        }

        // The allocateXposedObject bypass below exists specifically for the
        // Tenpay-JNI-crashes-in-cloned-profiles bug seen on the sjmyzq device
        // (Android 16 dual-instance). LSPosed v2.2.0 (seen on phone-a with
        // WeChat 8.0.78) no longer exposes HookBridge to a module's own
        // ClassLoader the way earlier LSPosed builds did, and phone-a is not a
        // cloned-profile device, so try the real constructor first -- it is
        // very likely fine here -- and only fall back to the allocate-and-
        // reproduce-the-constructor hack if that genuinely fails the way it
        // did on the original bug device.
        try {
            return constructor.newInstance(args);
        } catch (Throwable directConstructionFailure) {
            log("transfer scene direct construction failed, falling back to allocate+reproduce: "
                    + shortError(directConstructionFailure));
        }

        Object scene = allocateXposedObject(sceneClass);
        initializeTransferSceneDefaults(scene);
        Map<String, String> request = new HashMap<>();
        if ("com.tencent.mm.plugin.remittance.model.h1".equals(className)) {
            initializeTransferQueryScene(scene, args, request);
        } else {
            initializeTransferConfirmScene(scene, args, request);
        }
        prepareTransferRequest(scene, request, sceneClass.getClassLoader());
        if (TENPAY_SIGN_FALLBACK_TRANSFER_USED.compareAndSet(false, true)) {
            log("constructed transfer NetScene with Java WCPaySign class=" + className);
        }
        return scene;
    }

    private static Object allocateXposedObject(Class<?> targetClass) throws Exception {
        Class<?> hookBridge = findHookBridgeClass();
        Object allocated = findMethod(hookBridge, "allocateObject", Class.class)
                .invoke(null, targetClass);
        if (allocated == null) {
            throw new IllegalStateException("LSPosed failed to allocate " + targetClass.getName());
        }
        return allocated;
    }

    /**
     * Per handoff doc: on LSPosed v2.2.0 (seen on phone-a with WeChat 8.0.78),
     * none of these candidate ClassLoaders can see
     * org.lsposed.lspd.core.nativebridge.HookBridge the way earlier LSPosed
     * builds exposed it to a module's own ClassLoader -- this module's
     * ClassLoader (real runtime class name "n1") only has its own dex in its
     * DexPathList, with no reference to whatever ClassLoader holds the
     * framework's internal implementation classes. allocateXposedObject is
     * only reached as a fallback now (see newTransferSceneWithJavaSignature),
     * for devices where direct construction still crashes the way it did on
     * the original sjmyzq bug device, so this keeps trying the same candidates
     * in case a future LSPosed build (or a different module/Xposed
     * implementation) does expose it through one of them.
     */
    private static Class<?> findHookBridgeClass() throws ClassNotFoundException {
        String className = "org.lsposed.lspd.core.nativebridge.HookBridge";
        ClassLoader[] candidates = {
                XposedBridge.class.getClassLoader(),
                HookEntry.class.getClassLoader(),
                ClassLoader.getSystemClassLoader(),
                Thread.currentThread().getContextClassLoader(),
        };
        ClassNotFoundException lastError = null;
        for (ClassLoader candidate : candidates) {
            if (candidate == null) {
                continue;
            }
            try {
                return Class.forName(className, false, candidate);
            } catch (ClassNotFoundException e) {
                lastError = e;
            }
        }
        throw lastError != null ? lastError : new ClassNotFoundException(className);
    }

    /**
     * HookBridge.allocateObject skips every constructor in the n0/h1 -> o -> z0 ->
     * d1 -> m1 chain, so none of m1's field initializers run. Per handoff doc
     * 13.39, dexdump of the Tinker-patched com.tencent.mm.modelbase.m1.<init>
     * shows it sets netId = -1 (a "not yet dispatched" sentinel) and
     * limit = -99 (LIMIT_NOT_INITIALIZED); a raw-allocated object instead has
     * both at Java's int default of 0. If the real dispatch/queue code branches
     * on netId != -1 to decide whether a scene has already been sent, our scenes
     * would look "already dispatched" from the moment they're created and the
     * request would never actually reach the network -- callback timeout,
     * with zero errors anywhere, exactly matching what's been observed. Restore
     * both sentinels here so the scene looks freshly-constructed to that check.
     */
    private static void initializeTransferSceneDefaults(Object scene) throws Exception {
        setObjectField(scene, "", "mProcessName");
        setObjectField(scene, "", "payFlag");
        setObjectField(scene, "", "payAppUrl");
        setObjectField(scene, "", "wappay_jumped_url");
        setObjectField(scene, "", "crt_token");
        setObjectField(scene, new HashMap<String, String>(), "_request");
        setIntField(scene, "netId", -1);
        setIntField(scene, "limit", -99);
    }

    private static void initializeTransferQueryScene(
            Object scene,
            Object[] args,
            Map<String, String> request) throws Exception {
        int queryType = ((Number) args[0]).intValue();
        String transactionId = (String) args[1];
        String transferId = (String) args[2];
        int invalidTime = ((Number) args[3]).intValue();
        String groupUsername = (String) args[4];
        String attach = (String) args[5];

        setObjectField(scene, transferId, "d");
        setIntField(scene, "q", queryType);
        setObjectField(scene, "", "G");
        request.put("transfer_id", transferId);
        request.put("trans_id", transactionId);
        request.put("invalid_time", String.valueOf(invalidTime));
        request.put("group_username", groupUsername);
        putEncodedTransferAttach(request, attach);
    }

    private static void initializeTransferConfirmScene(
            Object scene,
            Object[] args,
            Map<String, String> request) throws Exception {
        String transactionId = (String) args[0];
        String transferId = (String) args[1];
        int amountMinor = ((Number) args[2]).intValue();
        String operation = (String) args[3];
        String username = (String) args[4];
        int invalidTime = ((Number) args[5]).intValue();
        String groupUsername = (String) args[6];
        int receiveAccountType = ((Number) args[7]).intValue();
        String bindSerial = (String) args[8];
        long subReceiveChannelId = ((Number) args[10]).longValue();
        String receiver = (String) args[11];
        String attach = (String) args[12];

        setObjectField(scene, operation, "d");
        setObjectField(scene, "", "g");
        setObjectField(scene, "", "h");
        setObjectField(scene, receiver, "m");
        setObjectField(scene, groupUsername, "n");
        request.put("trans_id", transferId);
        request.put("transaction_id", transactionId);
        request.put("op", operation);
        request.put("username", username);
        request.put("invalid_time", String.valueOf(invalidTime));
        request.put("total_fee", String.valueOf(amountMinor));
        request.put("left_button_continue", "");
        request.put("group_username", groupUsername);
        request.put("sub_recv_channel_id", String.valueOf(subReceiveChannelId));
        request.put("recv_account_type", String.valueOf(receiveAccountType));
        if (!isBlank(groupUsername)) {
            request.put("group_type", isChatroomTalker(groupUsername) ? "2" : "1");
        }
        if (!isBlank(bindSerial)) {
            request.put("bind_serial", bindSerial);
        }
        putEncodedTransferAttach(request, attach);
    }

    private static void putEncodedTransferAttach(Map<String, String> request, String attach)
            throws Exception {
        if (!isBlank(attach)) {
            request.put("transfer_attach", java.net.URLEncoder.encode(
                    attach,
                    StandardCharsets.UTF_8.name()));
        }
    }

    private static void prepareTransferRequest(
            Object scene,
            Map<String, String> request,
            ClassLoader classLoader) throws Exception {
        request.remove("uin");
        Class<?> walletRequestKey = findClass(classLoader, "o85.y");
        Object hasRequestKey = findNoArgMethod(walletRequestKey, "c").invoke(null);
        if (hasRequestKey instanceof Boolean && !((Boolean) hasRequestKey)) {
            Object jsapiRequestKey = findField(walletRequestKey, "a").get(null);
            if (jsapiRequestKey instanceof String) {
                request.put("jsapi_reqkey", (String) jsapiRequestKey);
            }
        }
        findField(scene.getClass(), "_request").set(scene, new HashMap<>(request));
        findMethod(scene.getClass(), "configRequest", boolean.class, boolean.class)
                .invoke(scene, true, true);

        Object requestResponse = findField(scene.getClass(), "rr").get(scene);
        if (requestResponse == null) {
            throw new IllegalStateException("transfer request/response was not initialized");
        }
        Object requestObject = findNoArgMethod(requestResponse.getClass(), "getReqObj")
                .invoke(requestResponse);
        findMethod(requestObject.getClass(), "setRouteInfo", int.class)
                .invoke(requestObject, 0);
        findMethod(findClass(classLoader, "com.tencent.mm.wallet_core.ui.r1"), "L", String.class)
                .invoke(null, request.get("req_key"));

        String signedRequest = TenpaySignFallback.appendRequestSignature(request);
        Class<?> byteStringClass = findClass(classLoader, "y85.sz5");
        Object byteString = findConstructor(byteStringClass).newInstance();
        findMethod(byteStringClass, "d", byte[].class)
                .invoke(byteString, signedRequest.getBytes(StandardCharsets.UTF_8));
        findMethod(
                scene.getClass(),
                "putToReqText",
                findClass(classLoader, "com.tencent.mm.modelbase.o"),
                byteStringClass)
                .invoke(scene, requestResponse, byteString);
    }

    private static void selfTestTransferSceneConstruction(ClassLoader classLoader) {
        if (!TENPAY_TRANSFER_SCENE_SELF_TESTED.compareAndSet(false, true)) {
            return;
        }
        try {
            Class<?> queryClass = findClass(
                    classLoader,
                    "com.tencent.mm.plugin.remittance.model.h1");
            Constructor<?> queryConstructor = findConstructor(
                    queryClass,
                    int.class,
                    String.class,
                    String.class,
                    int.class,
                    String.class,
                    String.class);
            Object query = newTransferScene(
                    queryConstructor,
                    1,
                    "self-test-transaction",
                    "self-test-transfer",
                    0,
                    "",
                    "");
            if (findField(query.getClass(), "rr").get(query) == null) {
                throw new IllegalStateException("query request was not initialized");
            }

            Class<?> confirmClass = findClass(
                    classLoader,
                    "com.tencent.mm.plugin.remittance.model.n0");
            Constructor<?> confirmConstructor = findConstructor(
                    confirmClass,
                    String.class,
                    String.class,
                    int.class,
                    String.class,
                    String.class,
                    int.class,
                    String.class,
                    int.class,
                    String.class,
                    Map.class,
                    long.class,
                    String.class,
                    String.class);
            Object confirm = newTransferScene(
                    confirmConstructor,
                    "self-test-transaction",
                    "self-test-transfer",
                    1,
                    "confirm",
                    "self-test-user",
                    0,
                    "",
                    1,
                    "",
                    null,
                    0L,
                    "self-test-user",
                    "");
            if (findField(confirm.getClass(), "rr").get(confirm) == null) {
                throw new IllegalStateException("confirm request was not initialized");
            }
            log("transfer NetScene Java-signature construction self-test passed");
        } catch (Throwable t) {
            log("transfer NetScene Java-signature construction self-test failed: "
                    + fullErrorChain(t));
        }
    }

    private static void logTenpayTransferFallback(String hookPoint) {
        if (Boolean.TRUE.equals(TENPAY_SIGN_FALLBACK_TRANSFER_ACTIVE.get())
                && TENPAY_SIGN_FALLBACK_TRANSFER_USED.compareAndSet(false, true)) {
            log("transfer request entered Java Tenpay fallback at " + hookPoint);
        }
    }

    private static void selfTestTenpaySignFallback(ClassLoader classLoader) {
        if (!TENPAY_SIGN_FALLBACK_SELF_TESTED.compareAndSet(false, true)) {
            return;
        }
        Boolean previous = TENPAY_SIGN_FALLBACK_ACTIVE.get();
        TENPAY_SIGN_FALLBACK_ACTIVE.set(Boolean.TRUE);
        try {
            String expected = TenpaySignFallback.sign("abc");
            Class<?> tenpayUtilClass = findClass(classLoader, "com.tenpay.android.wechat.TenpayUtil");
            Object actual = findMethod(tenpayUtilClass, "signWith3Des", String.class)
                    .invoke(null, "abc");
            if (!expected.equals(actual)) {
                throw new IllegalStateException("Tenpay fallback self-test signature mismatch");
            }
            log("scoped Tenpay sign fallback self-test passed");
        } catch (Throwable t) {
            log("scoped Tenpay sign fallback self-test failed: " + fullErrorChain(t));
        } finally {
            if (previous == null) {
                TENPAY_SIGN_FALLBACK_ACTIVE.remove();
            } else {
                TENPAY_SIGN_FALLBACK_ACTIVE.set(previous);
            }
        }
    }

    private static void deoptimizeTenpayCallers(ClassLoader classLoader) {
        if (!TENPAY_CALLERS_DEOPTIMIZED.compareAndSet(false, true)) {
            return;
        }
        try {
            Method deoptimize = findMethod(XposedBridge.class, "deoptimizeMethod", Member.class);
            int count = 0;
            count += deoptimizeDeclaredMethods(
                    deoptimize,
                    findClass(classLoader, "com.tenpay.android.wechat.TenpayUtil"),
                    "signWith3Des");
            count += deoptimizeDeclaredMethods(
                    deoptimize,
                    findClass(classLoader, "com.tenpay.ndk.Encrypt"),
                    "desedeEncode");
            count += deoptimizeDeclaredMethods(
                    deoptimize,
                    findClass(classLoader, "com.tenpay.ndk.Encrypt"),
                    "encrypt");
            count += deoptimizeDeclaredMethods(
                    deoptimize,
                    findClass(classLoader, "com.tencent.mm.wallet_core.tenpay.model.o"),
                    "getEncryptUrl");
            count += deoptimizeDeclaredMethods(
                    deoptimize,
                    findClass(classLoader, "com.tencent.mm.wallet_core.model.z0"),
                    "setRequestData");
            count += deoptimizeConstructors(
                    deoptimize,
                    findClass(classLoader, "com.tencent.mm.plugin.remittance.model.h1"));
            count += deoptimizeConstructors(
                    deoptimize,
                    findClass(classLoader, "com.tencent.mm.plugin.remittance.model.n0"));
            log("deoptimized Tenpay transfer callers count=" + count);
        } catch (Throwable t) {
            log("deoptimize Tenpay transfer callers failed: " + fullErrorChain(t));
        }
    }

    private static int deoptimizeDeclaredMethods(
            Method deoptimize,
            Class<?> targetClass,
            String methodName) throws Exception {
        int count = 0;
        for (Method method : targetClass.getDeclaredMethods()) {
            if (!methodName.equals(method.getName()) || Modifier.isAbstract(method.getModifiers())) {
                continue;
            }
            method.setAccessible(true);
            deoptimize.invoke(null, method);
            count++;
        }
        return count;
    }

    private static int deoptimizeConstructors(Method deoptimize, Class<?> targetClass) throws Exception {
        int count = 0;
        for (Constructor<?> constructor : targetClass.getDeclaredConstructors()) {
            constructor.setAccessible(true);
            deoptimize.invoke(null, constructor);
            count++;
        }
        return count;
    }

    private static void logWalletClassLoaderDiagnostics(ClassLoader classLoader) {
        if (classLoader == null || !WALLET_CLASSLOADERS_LOGGED.compareAndSet(false, true)) {
            return;
        }
        String[] classNames = {
                "lp.a0",
                "lp.d0",
                "sd5.n0",
                "ni0.j2",
                "com.tenpay.android.wechat.TenpayUtil",
                "com.tenpay.ndk.Encrypt",
                "com.tencent.mm.plugin.remittance.model.h1",
        };
        log("wallet classloader diag: runtime=" + describeClassLoader(classLoader));
        for (String className : classNames) {
            try {
                Class<?> targetClass = Class.forName(className, false, classLoader);
                log("wallet classloader diag: " + className + "="
                        + describeClassLoader(targetClass.getClassLoader()));
            } catch (Throwable t) {
                log("wallet classloader diag: " + className + " unavailable: " + shortError(t));
            }
        }
    }

    private static String describeClassLoader(ClassLoader classLoader) {
        if (classLoader == null) {
            return "<bootstrap>";
        }
        return classLoader.getClass().getName() + "@"
                + Integer.toHexString(System.identityHashCode(classLoader));
    }

    private static void hookApplicationAttach(final ClassLoader classLoader) {
        try {
            XposedHelpers.findAndHookMethod(
                    Application.class,
                    "attach",
                    Context.class,
                    new XC_MethodHook() {
                        @Override
                        protected void afterHookedMethod(MethodHookParam param) {
                            if (param.args != null && param.args.length > 0 && param.args[0] instanceof Context) {
                                APP_CONTEXT = (Context) param.args[0];
                                hookExternalStorageFallback();
                                ClassLoader runtimeLoader = param.thisObject == null ? null : param.thisObject.getClass().getClassLoader();
                                ensureAppContextField(APP_CONTEXT, runtimeLoader == null ? classLoader : runtimeLoader);
                                ensureWeChatBuildInfo(APP_CONTEXT, runtimeLoader == null ? classLoader : runtimeLoader);
                                log("Application.attach captured context; start outbox worker");
                                startWorker(runtimeLoader == null ? classLoader : runtimeLoader);
                            }
                        }
                    });
            log("hooked Application.attach");
        } catch (Throwable t) {
            log("hook Application.attach failed: " + t);
        }
    }

    private static void hookWeChatApplication(final ClassLoader classLoader) {
        try {
            XposedHelpers.findAndHookMethod(
                    "com.tencent.mm.app.MMApplicationLike",
                    classLoader,
                    "onBaseContextAttached",
                    Context.class,
                    new XC_MethodHook() {
                        @Override
                        protected void afterHookedMethod(MethodHookParam param) {
                            if (param.args != null && param.args.length > 0 && param.args[0] instanceof Context) {
                                APP_CONTEXT = (Context) param.args[0];
                                hookExternalStorageFallback();
                            }
                            ClassLoader runtimeLoader = param.thisObject == null ? null : param.thisObject.getClass().getClassLoader();
                            ensureAppContextField(APP_CONTEXT, runtimeLoader == null ? classLoader : runtimeLoader);
                            ensureWeChatBuildInfo(APP_CONTEXT, runtimeLoader == null ? classLoader : runtimeLoader);
                            log("WeChat application init completed; start outbox worker");
                            startWorker(runtimeLoader == null ? classLoader : runtimeLoader);
                        }
                    });
            XposedHelpers.findAndHookMethod(
                    "com.tencent.mm.app.MMApplicationLike",
                    classLoader,
                    "onCreate",
                    new XC_MethodHook() {
                        @Override
                        protected void afterHookedMethod(MethodHookParam param) {
                            Object app = currentApplication();
                            if (app instanceof Context) {
                                APP_CONTEXT = (Context) app;
                                hookExternalStorageFallback();
                            }
                            ClassLoader runtimeLoader = param.thisObject == null ? null : param.thisObject.getClass().getClassLoader();
                            ensureAppContextField(APP_CONTEXT, runtimeLoader == null ? classLoader : runtimeLoader);
                            ensureWeChatBuildInfo(APP_CONTEXT, runtimeLoader == null ? classLoader : runtimeLoader);
                            logWalletClassLoaderDiagnostics(runtimeLoader == null ? classLoader : runtimeLoader);
                            log("WeChat application onCreate completed; start outbox worker");
                            startWorker(runtimeLoader == null ? classLoader : runtimeLoader);
                        }
                    });
            log("hooked WeChat application init");
            startDelayedWorker(classLoader, 20000L);
        } catch (Throwable t) {
            log("hook WeChat application init failed, start worker with readiness gate: " + t);
            startWorker(classLoader);
        }
    }

    private static void startDelayedWorker(final ClassLoader classLoader, final long delayMs) {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                if (!sleepOnce(delayMs)) {
                    return;
                }
                log("delayed worker fallback fired");
                startWorker(classLoader);
            }
        }, "wechat-observatory-delayed-start");
        thread.setDaemon(true);
        thread.start();
    }

    private static void handleInsert(XC_MethodHook.MethodHookParam param) {
        try {
            if (!isTargetAndroidUser(BridgeConfig.load(bridgeContext()))) {
                return;
            }
            String table = stringArg(param.args, 0);
            if (param.thisObject != null && "message".equals(table)) {
                LAST_DATABASE = param.thisObject;
            }
            if (!"message".equals(table)) {
                return;
            }
            if (!(param.args[2] instanceof ContentValues)) {
                return;
            }

            ContentValues values = (ContentValues) param.args[2];
            String talker = values.getAsString("talker");
            String content = values.getAsString("content");
            Integer isSend = values.getAsInteger("isSend");
            Long msgId = values.getAsLong("msgId");
            Long msgSvrId = values.getAsLong("msgSvrId");
            Long createTime = values.getAsLong("createTime");
            Integer type = values.getAsInteger("type");
            String imgPath = values.getAsString("imgPath");
            String msgSource = values.getAsString("msgSource");

            int messageType = type == null ? 1 : type;
            if (!shouldReportMessage(talker, content, messageType)) {
                return;
            }

            BridgeConfig config = BridgeConfig.load(bridgeContext());
            if (!isTargetAndroidUser(config)) {
                return;
            }
            if (!config.enabled || isBlank(config.baseUrl) || isBlank(config.apiKey)) {
                return;
            }
            if (!bindRuntimeIdentity(config)) {
                return;
            }
            if (!ensureRegistered(config)) {
                return;
            }

            String mediaHint = resolveMediaHint(param.thisObject, messageType, msgId, msgSvrId, imgPath);
            MessagePayload payload = buildMessagePayload(
                    config, talker, content, isSend, msgId, createTime, messageType, mediaHint, msgSource);
            if (!shouldDeferOriginalImagePost(config, messageType, payload)) {
                postAsync(config, payload, "handle insert");
            }
            scheduleMediaRetryIfNeeded(config, talker, content, isSend, msgId, msgSvrId, createTime, messageType, mediaHint, payload);
            if (msgId != null && msgId > LAST_MESSAGE_ID) {
                LAST_MESSAGE_ID = msgId;
                MESSAGE_WATERMARK_READY = true;
            }
        } catch (Throwable t) {
            log("handle insert failed: " + t);
        }
    }

    private static void post(BridgeConfig config, MessagePayload payload) throws Exception {
        postJson(config, "/webhook/lsposed/message", payload.toJson());
    }

    private static void postAsync(final BridgeConfig config, final MessagePayload payload, final String context) {
        if (config == null || payload == null) {
            return;
        }
        POST_EXECUTOR.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    post(config, payload);
                    rememberMediaUpload(payload);
                } catch (Throwable t) {
                    log(context + " async upload failed: " + shortError(t));
                }
            }
        });
    }

    private static void startWorker(final ClassLoader classLoader) {
        if (!WORKER_STARTED.compareAndSet(false, true)) {
            return;
        }
        startOutboxWorker(classLoader);
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                runWorker(classLoader);
            }
        }, "wechat-observatory-worker");
        thread.setDaemon(true);
        thread.start();
    }

    private static void runWorker(ClassLoader classLoader) {
        WECHAT_CLASS_LOADER = classLoader;
        while (true) {
            BridgeConfig config = BridgeConfig.load(bridgeContext());
            try {
                if (!isTargetAndroidUser(config)) {
                    logTargetUserSkip("stop worker");
                    return;
                }
                if (config.enabled && !isBlank(config.baseUrl) && !isBlank(config.apiKey)) {
                    if (!bindRuntimeIdentity(config)) {
                        if (!sleepOnce(Math.max(3000L, config.pollIntervalMs))) {
                            return;
                        }
                        continue;
                    }
                    if (!ensureRegistered(config)) {
                        if (!sleepOnce(Math.max(3000L, config.pollIntervalMs))) {
                            return;
                        }
                        continue;
                    }
                    if (!isWeChatReadyForSend(classLoader)) {
                        log("WeChat send stack not ready; skip outbox poll");
                    }
                    // Message upload and contact sync do NOT depend on the send stack.
                    // The send stack check gates outbox (server→phone), but pollMessagesIfDue
                    // only reads the local DB and posts to the server — decouple it so
                    // message reporting is never frozen by send-stack initialization delays.
                    syncContactsIfDue(config);
                    pollMessagesIfDue(config);
                }
            } catch (Throwable t) {
                log("worker failed: " + t);
            }
            if (!sleepOnce(Math.max(1000L, config.pollIntervalMs))) {
                return;
            }
        }
    }

    private static void startOutboxWorker(final ClassLoader classLoader) {
        if (!OUTBOX_WORKER_STARTED.compareAndSet(false, true)) {
            return;
        }
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                runOutboxWorker(classLoader);
            }
        }, "wechat-observatory-outbox");
        thread.setDaemon(true);
        thread.start();
    }

    private static void runOutboxWorker(ClassLoader classLoader) {
        while (true) {
            BridgeConfig config = BridgeConfig.load(bridgeContext());
            try {
                if (!isTargetAndroidUser(config)) {
                    logTargetUserSkip("stop outbox worker");
                    return;
                }
                if (config.enabled && !isBlank(config.baseUrl) && !isBlank(config.apiKey)) {
                    if (!bindRuntimeIdentity(config)) {
                        if (!sleepOnce(Math.max(3000L, config.pollIntervalMs))) {
                            return;
                        }
                        continue;
                    }
                    if (!ensureRegistered(config)) {
                        if (!sleepOnce(Math.max(3000L, config.pollIntervalMs))) {
                            return;
                        }
                        continue;
                    }
                    if (!isWeChatReadyForSend(classLoader)) {
                        log("WeChat send stack not ready; skip outbox websocket");
                    } else if (!runOutboxWebSocket(config, classLoader)) {
                        pollOutbox(config, classLoader);
                    }
                }
            } catch (Throwable t) {
                log("outbox worker failed: " + t);
            }
            if (!sleepOnce(Math.max(1000L, config.pollIntervalMs))) {
                return;
            }
        }
    }

    private static boolean sleepOnce(long millis) {
        try {
            Thread.sleep(millis);
            return true;
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static boolean isTargetAndroidUser(BridgeConfig config) {
        return config == null
                || config.targetAndroidUserId < 0
                || config.targetAndroidUserId == currentAndroidUserId();
    }

    private static int currentAndroidUserId() {
        int uid = android.os.Process.myUid();
        return uid < 100000 ? 0 : uid / 100000;
    }

    private static void logTargetUserSkip(String message) {
        long now = System.currentTimeMillis();
        if (now - LAST_USER_SKIP_LOG_AT < 30000L) {
            return;
        }
        LAST_USER_SKIP_LOG_AT = now;
        BridgeConfig config = BridgeConfig.load(null);
        log(message + ": currentAndroidUserId=" + currentAndroidUserId()
                + " targetAndroidUserId=" + (config == null ? -1 : config.targetAndroidUserId));
    }

    private static boolean isWeChatReadyForSend(ClassLoader classLoader) {
        try {
            classLoader = runtimeClassLoader(classLoader);
            WECHAT_CLASS_LOADER = classLoader;
            WeChatVersionSymbols.Profile symbols = weChatSymbols(classLoader);
            ensureWeChatRegistries(classLoader, symbols);
            if (symbols.extensionRegistryRequired
                    && !isStaticArraySlotReady(classLoader, "fs.g", "f283324a", "a")) {
                return readyState(false, "fs.g extension registry not initialized");
            }
            String serviceManager = symbols.className("i95.n0");
            if (!isStaticBooleanFlag(classLoader, serviceManager, "f307062f", "f")) {
                return readyState(false, serviceManager + " service manager not initialized");
            }
            logWalletClassLoaderDiagnostics(classLoader);
            ensureTenpaySignFallbackHook(classLoader);
            deoptimizeTenpayCallers(classLoader);
            selfTestTenpaySignFallback(classLoader);
            selfTestTransferSceneConstruction(classLoader);
            return readyState(true, "ready");
        } catch (Throwable t) {
            return readyState(false, "readiness check failed: " + shortError(t));
        }
    }

    private static final class NetworkResultWaiter {
        final CountDownLatch latch = new CountDownLatch(1);
        volatile int code = Integer.MIN_VALUE;
        volatile String message = "";

        void complete(Object[] args) {
            if (args != null && args.length > 0 && args[0] instanceof Number) {
                code = ((Number) args[0]).intValue();
            }
            if (args != null && args.length > 1 && args[1] != null) {
                message = String.valueOf(args[1]);
            }
            latch.countDown();
        }

        boolean await(long timeoutSeconds) throws InterruptedException {
            return latch.await(timeoutSeconds, TimeUnit.SECONDS);
        }

        boolean isComplete() {
            return latch.getCount() == 0L;
        }
    }

    private static void ensureWeChatRegistries(
            ClassLoader classLoader,
            WeChatVersionSymbols.Profile symbols) throws Exception {
        classLoader = runtimeClassLoader(classLoader);
        // See ensureAppContextField's comment: "x2" was reassigned to an unrelated
        // class in WeChat 8.0.78, with the real holder renamed to "a3" (setter
        // method "u" -> "w"). findClassWithField's static-field check exists
        // precisely because 8.0.78's "x2" happens to have its own unrelated
        // same-named instance field "a", which a bare name lookup would wrongly
        // accept and then crash on with "null receiver".
        Class<?> appContext = findClassWithField(
                classLoader,
                new String[]{"com.tencent.mm.sdk.platformtools.x2", "com.tencent.mm.sdk.platformtools.a3"},
                "f210311a", "a");
        Object application = findFieldAny(appContext, "f210311a", "a").get(null);
        if (application == null) {
            application = currentApplication();
        }
        if (application == null) {
            log("WeChat application context is null during registry init");
            return;
        }
        if (findFieldAny(appContext, "f210311a", "a").get(null) == null) {
            findMethodAny(appContext, new Class<?>[]{Context.class}, "u", "w").invoke(null, application);
            log("initialized WeChat MMApplicationContext from module");
        }

        if (symbols.extensionRegistryRequired) {
            Class<?> extensionRegistry = findClass(classLoader, "fs.g");
            Field registryField = findFieldAny(extensionRegistry, "f283324a", "a");
            Object registryArray = registryField.get(null);
            if (registryArray != null
                    && registryArray.getClass().isArray()
                    && java.lang.reflect.Array.getLength(registryArray) > 0
                    && java.lang.reflect.Array.get(registryArray, 0) == null) {
                java.lang.reflect.Array.set(registryArray, 0, enumConstant(classLoader, "fs.k2", "INSTANCE"));
                findFieldAny(extensionRegistry, "f283326c", "c").set(null, application);
                findFieldAny(extensionRegistry, "f283325b", "b").set(
                        null,
                        enumConstant(classLoader, "com.tencent.mm.app.q0", "INSTANCE"));
                log("initialized fs.g extension registry from module");
            }
        }

        String serviceManagerName = symbols.className("i95.n0");
        if (symbols.forceServiceInitialization
                && !isStaticBooleanFlag(classLoader, serviceManagerName, "f307062f", "f")) {
            Class<?> providerClass = findClass(classLoader, "com.tencent.mm.app.p0");
            Object provider = findFieldAny(providerClass, "f70808d", "d").get(null);
            Object y = findMethod(provider.getClass(), "b").invoke(provider);
            Method initialize = findMethod(
                    findClass(classLoader, serviceManagerName),
                    "d",
                    Application.class,
                    findClass(classLoader, symbols.className("i95.y")),
                    findClass(classLoader, symbols.className("k95.a")));
            initialize.invoke(null, application, y, enumConstant(classLoader, "com.tencent.mm.app.q0", "INSTANCE"));
            log("initialized " + serviceManagerName + " service manager from module");
        }
    }

    private static Object enumConstant(ClassLoader classLoader, String className, String name) throws Exception {
        Class<?> enumClass = findClass(classLoader, className);
        Object[] constants = enumClass.getEnumConstants();
        if (constants != null && constants.length > 0) {
            for (Object constant : constants) {
                if (name.equals(String.valueOf(constant))) {
                    return constant;
                }
            }
            return constants[0];
        }
        Field field = findFieldAny(enumClass, name);
        return field.get(null);
    }

    private static Object currentApplication() {
        try {
            Class<?> activityThread = Class.forName("android.app.ActivityThread");
            Object app = findNoArgMethod(activityThread, "currentApplication").invoke(null);
            if (app != null) {
                return app;
            }
            Object thread = findNoArgMethod(activityThread, "currentActivityThread").invoke(null);
            return thread == null ? null : findNoArgMethod(thread.getClass(), "getApplication").invoke(thread);
        } catch (Throwable t) {
            log("read current application failed: " + shortError(t));
            return null;
        }
    }

    private static Context bridgeContext() {
        Context context = APP_CONTEXT;
        if (context != null) {
            return context;
        }
        Object app = currentApplication();
        return app instanceof Context ? (Context) app : null;
    }

    private static ClassLoader runtimeClassLoader(ClassLoader fallback) {
        List<ClassLoader> candidates = new ArrayList<>();
        addClassLoader(candidates, Thread.currentThread().getContextClassLoader());
        Object app = currentApplication();
        addClassLoader(candidates, app instanceof Application ? ((Application) app).getClassLoader() : null);
        addClassLoader(candidates, app == null ? null : app.getClass().getClassLoader());
        addClassLoader(candidates, fallback);

        ClassLoader loader = null;
        for (ClassLoader candidate : candidates) {
            if (candidate == null) {
                continue;
            }
            if (isPatchedWeChatLoader(candidate)) {
                loader = candidate;
                break;
            }
        }
        if (loader == null) {
            loader = firstLoadable(candidates);
        }
        if (loader == null) {
            return fallback;
        }
        String state = loader.toString();
        if (!state.equals(LAST_CLASSLOADER_STATE)) {
            LAST_CLASSLOADER_STATE = state;
            log("using WeChat runtime classLoader: " + state);
        }
        return loader;
    }

    private static void addClassLoader(List<ClassLoader> loaders, ClassLoader loader) {
        if (loader == null) {
            return;
        }
        for (ClassLoader existing : loaders) {
            if (existing == loader) {
                return;
            }
        }
        loaders.add(loader);
    }

    private static boolean isPatchedWeChatLoader(ClassLoader loader) {
        String description = String.valueOf(loader);
        return description.contains("DelegateLastClassLoader")
                || description.contains("tinker_classN")
                || description.contains("/tinker/");
    }

    private static ClassLoader firstLoadable(List<ClassLoader> candidates) {
        for (ClassLoader candidate : candidates) {
            try {
                detectWeChatSymbols(candidate);
                return candidate;
            } catch (Throwable ignored) {
                // Try the next candidate.
            }
        }
        return null;
    }

    private static WeChatVersionSymbols.Profile detectWeChatSymbols(final ClassLoader classLoader) {
        return WeChatVersionSymbols.resolve(new WeChatVersionSymbols.ClassProbe() {
            @Override
            public boolean isPresent(String className) {
                try {
                    Class.forName(className, false, classLoader);
                    return true;
                } catch (Throwable ignored) {
                    return false;
                }
            }
        });
    }

    private static WeChatVersionSymbols.Profile weChatSymbols(ClassLoader classLoader) {
        WeChatVersionSymbols.Profile symbols = WECHAT_SYMBOLS;
        if (symbols != null && WECHAT_SYMBOLS_CLASS_LOADER == classLoader) {
            return symbols;
        }
        symbols = detectWeChatSymbols(classLoader);
        WECHAT_SYMBOLS = symbols;
        WECHAT_SYMBOLS_CLASS_LOADER = classLoader;
        log("detected WeChat runtime symbols version=" + symbols.version);
        return symbols;
    }

    private static Class<?> findVersionedClass(ClassLoader classLoader, String baselineName)
            throws ClassNotFoundException {
        return findClass(classLoader, weChatSymbols(classLoader).className(baselineName));
    }

    private static boolean readyState(boolean ready, String reason) {
        String state = ready ? "ready" : reason;
        if (!state.equals(LAST_READY_STATE)) {
            LAST_READY_STATE = state;
            log("WeChat readiness: " + state);
        }
        return ready;
    }

    private static boolean isStaticArraySlotReady(ClassLoader classLoader, String className, String... fieldNames) throws Exception {
        Field field = findFieldAny(findClass(classLoader, className), fieldNames);
        Object value = field.get(null);
        if (value == null || !value.getClass().isArray() || java.lang.reflect.Array.getLength(value) == 0) {
            return false;
        }
        return java.lang.reflect.Array.get(value, 0) != null;
    }

    private static boolean isStaticBooleanFlag(ClassLoader classLoader, String className, String... fieldNames) throws Exception {
        Field field = findFieldAny(findClass(classLoader, className), fieldNames);
        Object value = field.get(null);
        if (value instanceof boolean[]) {
            boolean[] flags = (boolean[]) value;
            return flags.length > 0 && flags[0];
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return false;
    }

    private static boolean booleanNoArg(Class<?> cls, String methodName) throws Exception {
        Object result = findNoArgMethod(cls, methodName).invoke(null);
        return result instanceof Boolean && (Boolean) result;
    }

    private static boolean bindRuntimeIdentity(BridgeConfig config) {
        String wxid = "";
        String nickname = "";
        Object db = LAST_DATABASE;
        if (db == null) {
            db = findContactDatabaseOnMainThread(config);
        }
        WeChatIdentity identity = readWeChatIdentity(db);
        wxid = identity.wxid;
        nickname = identity.nickname;
        if (isBlank(wxid)) {
            wxid = CURRENT_WXID;
        }
        if (isBlank(nickname)) {
            nickname = CURRENT_NICKNAME;
        }
        if (isBlank(wxid)) {
            log("runtime identity not ready: current wxid cannot be detected yet");
            return false;
        }
        if (!isBlank(CURRENT_WXID) && !wxid.equals(CURRENT_WXID)) {
            REGISTERED_KEY = "";
            REGISTERED_DEVICE = "";
            LAST_REGISTER_SUCCESS_AT = 0L;
            log("wechat identity changed wxid=" + wxid);
        }
        CURRENT_WXID = wxid;
        CURRENT_NICKNAME = nickname;
        config.selfWxid = wxid;
        config.nickname = isBlank(nickname) ? wxid : nickname;
        return true;
    }

    private static WeChatIdentity readWeChatIdentity(Object db) {
        if (db == null) {
            return new WeChatIdentity("", "");
        }
        String wxid = "";
        String nickname = "";
        String[][] candidates = new String[][]{
                {"SELECT value FROM userinfo WHERE id=2 LIMIT 1", ""},
                {"SELECT value FROM userinfo WHERE id=42 LIMIT 1", ""},
                {"SELECT value FROM userinfo WHERE id=4 LIMIT 1", "nickname"},
                {"SELECT value FROM userinfo WHERE id=5 LIMIT 1", "nickname"},
                {"SELECT value FROM userinfo WHERE id=6 LIMIT 1", "nickname"}
        };
        for (String[] candidate : candidates) {
            String value = readSingleString(db, candidate[0]);
            if (isBlank(value)) {
                continue;
            }
            if ("nickname".equals(candidate[1])) {
                if (isBlank(nickname)) {
                    nickname = value;
                }
                continue;
            }
            if (looksLikeWxid(value)) {
                wxid = value;
                break;
            }
        }
        return new WeChatIdentity(wxid, nickname);
    }

    private static String readSingleString(Object db, String sql) {
        Object cursor = null;
        try {
            cursor = rawQuery(db, sql, new String[]{});
            if (cursor == null) {
                return "";
            }
            Method moveToFirst = findNoArgMethod(cursor.getClass(), "moveToFirst");
            if (Boolean.TRUE.equals(moveToFirst.invoke(cursor))) {
                return stringColumn(cursor, 0);
            }
        } catch (Throwable ignored) {
            // WeChat database schemas vary between versions; try the next candidate.
        } finally {
            closeQuietly(cursor);
        }
        return "";
    }

    private static final class WeChatIdentity {
        final String wxid;
        final String nickname;

        WeChatIdentity(String wxid, String nickname) {
            this.wxid = wxid == null ? "" : wxid.trim();
            this.nickname = nickname == null ? "" : nickname.trim();
        }
    }

    private static boolean ensureRegistered(BridgeConfig config) throws Exception {
        if (isBlank(config.apiKey) || isBlank(config.selfWxid)) {
            return false;
        }
        String key = config.apiKey + "\n" + config.selfWxid + "\n" + config.signature;
        long now = System.currentTimeMillis();
        if (key.equals(REGISTERED_KEY) && !isBlank(REGISTERED_DEVICE)) {
            config.device = REGISTERED_DEVICE;
            if (now - LAST_REGISTER_SUCCESS_AT < 60000L) {
                return true;
            }
        }
        if (now - LAST_REGISTER_ATTEMPT_AT < 5000L) {
            return false;
        }
        LAST_REGISTER_ATTEMPT_AT = now;
        return registerModule(config, key);
    }

    private static boolean registerModule(BridgeConfig config, String key) throws Exception {
        if (isBlank(config.apiKey) || isBlank(config.selfWxid)) {
            return false;
        }
        String body = "{"
                + "\"api_key\":\"" + json(config.apiKey) + "\","
                + "\"device\":\"" + json(config.device) + "\","
                + "\"wxid\":\"" + json(config.selfWxid) + "\","
                + "\"nickname\":\"" + json(config.nickname) + "\""
                + "}";
        String response = postJson(config, "/module/register", body);
        JSONObject root = new JSONObject(response);
        JSONObject result = root.optJSONObject("result");
        String device = "";
        if (result != null) {
            JSONObject deviceObject = result.optJSONObject("device");
            if (deviceObject != null) {
                device = deviceObject.optString("name", "");
            }
            if (isBlank(device)) {
                JSONObject account = result.optJSONObject("account");
                if (account != null) {
                    device = account.optString("device", "");
                }
            }
        }
        if (isBlank(device)) {
            log("module register response missing server device");
            return false;
        }
        REGISTERED_KEY = key;
        REGISTERED_DEVICE = device;
        LAST_REGISTER_SUCCESS_AT = System.currentTimeMillis();
        config.device = device;
        log("module registered device=" + device + " wxid=" + config.selfWxid);
        return true;
    }

    private static void syncContactsIfDue(BridgeConfig config) {
        if (config.contactSyncIntervalMs <= 0L) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now - LAST_CONTACT_SYNC_AT < config.contactSyncIntervalMs) {
            return;
        }
        Object db = LAST_DATABASE;
        if (db == null) {
            db = findContactDatabaseOnMainThread(config);
        }
        if (db == null) {
            if (now - LAST_CONTACT_SYNC_SKIP_LOG_AT > 60000L) {
                LAST_CONTACT_SYNC_SKIP_LOG_AT = now;
                log("contact sync skipped: WeChat database not captured yet");
            }
            return;
        }
        LAST_CONTACT_SYNC_AT = now;
        try {
            JSONArray contacts = readContacts(db, config);
            if (contacts.length() == 0) {
                log("contact sync skipped: no friend contacts read");
                return;
            }
            JSONObject body = new JSONObject();
            body.put("api_key", config.apiKey);
            body.put("device", config.device);
            body.put("wxid", config.selfWxid);
            body.put("complete", true);
            body.put("contacts", contacts);
            postJson(config, "/module/contacts/snapshot", body.toString());
            log("contact sync uploaded count=" + contacts.length()
                    + " includeChatrooms=" + config.includeChatrooms);
        } catch (Throwable t) {
            log("contact sync failed: " + shortError(t));
        }
    }

    private static void pollMessagesIfDue(BridgeConfig config) {
        long now = System.currentTimeMillis();
        if (now < MESSAGE_POLL_BACKOFF_UNTIL) {
            return;
        }
        if (now - LAST_MESSAGE_POLL_AT < Math.max(1000L, config.pollIntervalMs)) {
            return;
        }
        LAST_MESSAGE_POLL_AT = now;
        Object db = LAST_DATABASE;
        if (db == null) {
            db = findContactDatabaseOnMainThread(config);
        }
        if (db == null) {
            return;
        }
        try {
            if (!MESSAGE_WATERMARK_READY) {
                LAST_MESSAGE_ID = readMaxMessageId(db);
                MESSAGE_WATERMARK_READY = true;
                log("message poll watermark initialized msgId=" + LAST_MESSAGE_ID);
            }
            if (!TRANSFER_STARTUP_BACKFILL_DONE) {
                int recovered = backfillRecentPendingTransfers(db, config, LAST_MESSAGE_ID);
                TRANSFER_STARTUP_BACKFILL_DONE = true;
                if (recovered > 0) {
                    log("startup pending transfer backfill uploaded count=" + recovered);
                }
            }
            int count = pollNewMessages(db, config);
            if (count > 0) {
                log("message poll uploaded count=" + count + " lastMsgId=" + LAST_MESSAGE_ID);
            }
            MESSAGE_POLL_CONSECUTIVE_FAILS = 0;
        } catch (Throwable t) {
            handleMessagePollFailure(t);
        }
    }

    private static void handleMessagePollFailure(Throwable t) {
        long now = System.currentTimeMillis();
        MESSAGE_POLL_CONSECUTIVE_FAILS++;
        Throwable cause = rootCause(t);
        if (now - LAST_MESSAGE_POLL_FAIL_LOG_AT > 30000L || MESSAGE_POLL_CONSECUTIVE_FAILS <= 3) {
            LAST_MESSAGE_POLL_FAIL_LOG_AT = now;
            log("message poll failed count=" + MESSAGE_POLL_CONSECUTIVE_FAILS
                    + " error=" + shortError(cause));
        }
        if (MESSAGE_POLL_CONSECUTIVE_FAILS >= 3) {
            MESSAGE_POLL_BACKOFF_UNTIL = now + MESSAGE_POLL_BACKOFF_MS;
            log("message poll backoff seconds=" + (MESSAGE_POLL_BACKOFF_MS / 1000L)
                    + " reason=" + shortError(cause));
        }
    }

    private static long readMaxMessageId(Object db) throws Exception {
        Object cursor = rawQuery(db, "SELECT MAX(msgId) FROM message", new String[]{});
        if (cursor == null) {
            return 0L;
        }
        try {
            Method moveToFirst = findNoArgMethod(cursor.getClass(), "moveToFirst");
            if (Boolean.TRUE.equals(moveToFirst.invoke(cursor))) {
                return longColumn(cursor, 0);
            }
            return 0L;
        } finally {
            closeQuietly(cursor);
        }
    }

    private static int backfillRecentPendingTransfers(Object db, BridgeConfig config, long maxMessageId) throws Exception {
        if (maxMessageId <= 0L) {
            return 0;
        }
        Object cursor = rawQuery(db, ""
                + "SELECT msgId,talker,COALESCE(content,''),createTime,type "
                + "FROM message "
                + "WHERE msgId <= ? AND isSend=0 AND type IN (?,?) "
                + "ORDER BY msgId DESC "
                + "LIMIT " + TRANSFER_STARTUP_BACKFILL_LIMIT,
                new String[]{
                        String.valueOf(maxMessageId),
                        String.valueOf(MESSAGE_TYPE_TRANSFER),
                        String.valueOf(MESSAGE_TYPE_APPMSG)
                });
        if (cursor == null) {
            return 0;
        }
        int count = 0;
        long nowSeconds = System.currentTimeMillis() / 1000L;
        try {
            Method moveToNext = findNoArgMethod(cursor.getClass(), "moveToNext");
            while (Boolean.TRUE.equals(moveToNext.invoke(cursor))) {
                long msgId = longColumn(cursor, 0);
                String talker = stringColumn(cursor, 1);
                String content = stringColumn(cursor, 2);
                long createTime = normalizeCreateTime(longColumn(cursor, 3));
                int type = intColumn(cursor, 4);
                long ageSeconds = nowSeconds - createTime;
                if (createTime <= 0L || ageSeconds < -300L
                        || ageSeconds > TRANSFER_STARTUP_BACKFILL_WINDOW_SECONDS) {
                    continue;
                }
                String normalizedContent = normalizeMessageContent(talker, content, type);
                TransferMessageParser.Result transfer = TransferMessageParser.parse(normalizedContent);
                boolean transferType = type == MESSAGE_TYPE_TRANSFER
                        || (type == MESSAGE_TYPE_APPMSG
                        && transfer.appMsgType == TransferMessageParser.APPMSG_TYPE_TRANSFER);
                if (!transferType || !transfer.isPending()) {
                    continue;
                }
                MessagePayload payload = buildMessagePayload(
                        config,
                        talker,
                        content,
                        Integer.valueOf(0),
                        Long.valueOf(msgId),
                        Long.valueOf(createTime),
                        type,
                        "",
                        messageSourceForMessage(db, msgId));
                post(config, payload);
                count++;
            }
        } finally {
            closeQuietly(cursor);
        }
        return count;
    }

    private static int pollNewMessages(Object db, BridgeConfig config) throws Exception {
        int queryMode = 2;
        Object cursor;
        try {
            cursor = rawQuery(db, ""
                    + "SELECT msgId,COALESCE(msgSvrId,0),talker,COALESCE(content,''),isSend,createTime,type,COALESCE(imgPath,'') "
                    + "FROM message "
                    + "WHERE msgId > ? AND talker IS NOT NULL AND talker <> '' "
                    + "ORDER BY msgId ASC "
                    + "LIMIT 50", new String[]{String.valueOf(LAST_MESSAGE_ID)});
        } catch (Throwable t) {
            try {
                queryMode = 1;
                cursor = rawQuery(db, ""
                        + "SELECT msgId,talker,COALESCE(content,''),isSend,createTime,type,COALESCE(imgPath,'') "
                        + "FROM message "
                        + "WHERE msgId > ? AND talker IS NOT NULL AND talker <> '' "
                        + "ORDER BY msgId ASC "
                        + "LIMIT 50", new String[]{String.valueOf(LAST_MESSAGE_ID)});
            } catch (Throwable ignored) {
                queryMode = 0;
                cursor = rawQuery(db, ""
                        + "SELECT msgId,talker,COALESCE(content,''),isSend,createTime,type "
                        + "FROM message "
                        + "WHERE msgId > ? AND talker IS NOT NULL AND talker <> '' "
                        + "ORDER BY msgId ASC "
                        + "LIMIT 50", new String[]{String.valueOf(LAST_MESSAGE_ID)});
            }
        }
        if (cursor == null) {
            return 0;
        }
        int count = 0;
        try {
            Method moveToNext = findNoArgMethod(cursor.getClass(), "moveToNext");
            while (Boolean.TRUE.equals(moveToNext.invoke(cursor))) {
                int column = 0;
                long msgId = longColumn(cursor, column++);
                if (msgId <= LAST_MESSAGE_ID) {
                    continue;
                }

                long msgSvrId = queryMode == 2 ? longColumn(cursor, column++) : 0L;
                String talker = stringColumn(cursor, column++);
                String content = stringColumn(cursor, column++);
                int isSend = intColumn(cursor, column++);
                long createTime = normalizeCreateTime(longColumn(cursor, column++));
                int type = intColumn(cursor, column++);
                String imgPath = queryMode >= 1 ? stringColumn(cursor, column) : "";
                if (!shouldReportMessage(talker, content, type)) {
                    LAST_MESSAGE_ID = msgId;
                    continue;
                }

                int messageType = type <= 0 ? 1 : type;
                Long serverId = msgSvrId > 0L ? Long.valueOf(msgSvrId) : null;
                String mediaHint = resolveMediaHint(db, messageType, Long.valueOf(msgId), serverId, imgPath);
                String msgSource = messageSourceForMessage(db, msgId);
                MessagePayload payload = buildMessagePayload(
                        config,
                        talker,
                        content,
                        isSend == 1 ? Integer.valueOf(1) : Integer.valueOf(0),
                        msgId,
                        createTime,
                        messageType,
                        mediaHint,
                        msgSource);
                if (!shouldDeferOriginalImagePost(config, messageType, payload)) {
                    post(config, payload);
                }
                LAST_MESSAGE_ID = msgId;
                rememberMediaUpload(payload);
                scheduleMediaRetryIfNeeded(config, talker, content, isSend == 1 ? Integer.valueOf(1) : Integer.valueOf(0), msgId, serverId, createTime, messageType, mediaHint, payload);
                count++;
            }
        } finally {
            closeQuietly(cursor);
        }
        return count;
    }

    private static void scheduleMediaRetryIfNeeded(
            BridgeConfig config,
            String talker,
            String content,
            Integer isSend,
            Long msgId,
            Long msgSvrId,
            Long createTime,
            int type,
            String mediaHint,
            MessagePayload firstPayload) {
        if (!shouldScheduleMediaRetry(config, msgId, type, firstPayload)) {
            return;
        }
        final long recordId = msgId.longValue();
        if (!markMediaRetryScheduled(recordId)) {
            return;
        }
        if (type == 3) {
            requestOriginalImageDownloadAsync(recordId, msgSvrId, talker);
        } else if (type == 34) {
            requestVoiceDownloadAsync(recordId, msgSvrId, talker, mediaHint);
        } else if (type == 43 || type == 62) {
            requestVideoDownloadAsync(recordId, talker);
        }
        Thread worker = new Thread(new Runnable() {
            @Override
            public void run() {
                retryMediaUpload(recordId, msgSvrId, talker, content, isSend, createTime, type, mediaHint);
            }
        }, "WechatGatewayMediaRetry-" + recordId);
        worker.setDaemon(true);
        worker.start();
    }

    private static boolean shouldScheduleMediaRetry(BridgeConfig config, Long msgId, int type, MessagePayload firstPayload) {
        if (config == null || !config.mediaUploadEnabled || msgId == null || msgId <= 0) {
            return false;
        }
        if (hasRememberedMediaUpload(msgId.longValue())) {
            return false;
        }
        if (firstPayload != null && !isBlank(firstPayload.mediaBase64)) {
            return false;
        }
        String kind = mediaKindForMessageType(type);
        return !isBlank(kind);
    }

    private static void retryMediaUpload(long recordId, Long msgSvrId, String talker, String content, Integer isSend, Long createTime, int type, String mediaHint) {
        for (int i = 0; i < MEDIA_RETRY_DELAYS_MS.length; i++) {
            sleepQuietly(MEDIA_RETRY_DELAYS_MS[i]);
            if (hasRememberedMediaUpload(recordId)) {
                return;
            }
            try {
                BridgeConfig config = BridgeConfig.load(bridgeContext());
                if (!config.enabled || !config.mediaUploadEnabled || isBlank(config.baseUrl) || isBlank(config.apiKey)) {
                    return;
                }
                if (!isTargetAndroidUser(config) || !bindRuntimeIdentity(config) || !ensureRegistered(config)) {
                    return;
                }
                if (type == 3) {
                    requestOriginalImageDownloadAsync(recordId, msgSvrId, talker);
                } else if (type == 43 || type == 62) {
                    requestVideoDownloadAsync(recordId, talker);
                }
                String latestHint = resolveMediaHint(LAST_DATABASE, type, Long.valueOf(recordId), msgSvrId, mediaHint);
                if (type == 34) {
                    requestVoiceDownloadAsync(recordId, msgSvrId, talker, latestHint);
                }
                MessagePayload payload = buildMessagePayload(config, talker, content, isSend, recordId, createTime, type, latestHint);
                if (isBlank(payload.mediaBase64)) {
                    continue;
                }
                post(config, payload);
                rememberMediaUpload(payload);
                log("media retry uploaded type=" + type
                        + " msgId=" + recordId
                        + " attempt=" + (i + 1)
                        + " size=" + payload.mediaSize);
                return;
            } catch (Throwable t) {
                logMediaRetryFailure(recordId, type, i + 1, t);
            }
        }
        log("media retry exhausted type=" + type + " msgId=" + recordId);
    }

    private static boolean shouldDeferOriginalImagePost(BridgeConfig config, int type, MessagePayload payload) {
        if (type != 3 || config == null || !config.mediaUploadEnabled || payload == null
                || !isBlank(payload.mediaBase64)) {
            return false;
        }
        return supportsOriginalImageDownload();
    }

    private static boolean supportsOriginalImageDownload() {
        WeChatVersionSymbols.Profile symbols = WECHAT_SYMBOLS;
        if (symbols != null) {
            return symbols.originalImageDownloadSupported;
        }
        ClassLoader classLoader = WECHAT_CLASS_LOADER;
        if (classLoader == null) {
            return false;
        }
        try {
            return weChatSymbols(runtimeClassLoader(classLoader)).originalImageDownloadSupported;
        } catch (Throwable ignored) {
            return false;
        }
    }

    private static void requestOriginalImageDownloadAsync(final long recordId, final Long msgSvrId, final String talker) {
        if (recordId <= 0L || isBlank(talker) || !supportsOriginalImageDownload()
                || !markOriginalImageDownloadDispatched(recordId)) {
            return;
        }
        Runnable request = new Runnable() {
            @Override
            public void run() {
                try {
                    requestOriginalImageDownload(recordId, msgSvrId, talker);
                } catch (Throwable t) {
                    forgetOriginalImageDownloadDispatch(recordId);
                    log("original image download request failed msgId=" + recordId
                            + " error=" + shortError(rootCause(t)));
                }
            }
        };
        if (Looper.myLooper() == Looper.getMainLooper()) {
            request.run();
            return;
        }
        if (!new Handler(Looper.getMainLooper()).post(request)) {
            forgetOriginalImageDownloadDispatch(recordId);
        }
    }

    private static void requestOriginalImageDownload(long recordId, Long msgSvrId, String talker) throws Exception {
        ClassLoader classLoader = runtimeClassLoader(WECHAT_CLASS_LOADER);
        if (classLoader == null) {
            throw new IllegalStateException("WeChat class loader is unavailable");
        }
        ImageInfoRecord imageInfo = queryImageInfoRecord(
                LAST_DATABASE,
                Long.valueOf(recordId),
                msgSvrId);
        if (imageInfo == null || imageInfo.id <= 0L) {
            imageInfo = imageInfoFromWechatStorage(classLoader, recordId, msgSvrId, talker);
        }
        if (imageInfo == null || imageInfo.id <= 0L) {
            forgetOriginalImageDownloadDispatch(recordId);
            return;
        }

        int resourceType = imageInfo.hdId > 0L ? 1 : 0;
        ImageInfoRecord selected = imageInfo;
        if (resourceType == 1) {
            ImageInfoRecord hd = queryImageInfoRecordById(LAST_DATABASE, imageInfo.hdId);
            if (hd == null) {
                hd = imageInfoByIdFromWechatStorage(classLoader, imageInfo.hdId);
            }
            selected = hd == null ? new ImageInfoRecord() : hd;
        }
        if (!isBlank(completeOriginalImageHint(selected))) {
            return;
        }

        Class<?> serviceManagerClass = findVersionedClass(classLoader, "i95.n0");
        WeChatVersionSymbols.Profile symbols = weChatSymbols(classLoader);
        Class<?> imageFeatureClass = findVersionedClass(classLoader, "h70.y");
        Object imageFeature = findMethod(serviceManagerClass, "c", Class.class)
                .invoke(null, imageFeatureClass);
        Object downloadService = findNoArgMethod(imageFeature.getClass(), symbols.methods.imageDownloadService)
                .invoke(imageFeature);
        if (downloadService == null) {
            throw new IllegalStateException("WeChat image download service is unavailable");
        }

        Class<?> msgIdTalkerClass = findClass(classLoader, "com.tencent.mm.plugin.msg.MsgIdTalker");
        Object msgIdTalker = findConstructor(msgIdTalkerClass, long.class, String.class)
                .newInstance(recordId, talker);
        Class<?> callbackClass = findVersionedClass(classLoader, "h70.w");
        Object callback = Proxy.newProxyInstance(
                classLoader,
                new Class<?>[]{callbackClass},
                originalImageDownloadCallback(recordId));
        Method enqueue = findMethod(
                downloadService.getClass(),
                "b",
                long.class,
                msgIdTalkerClass,
                int.class,
                Object.class,
                int.class,
                callbackClass,
                int.class,
                boolean.class);
        Object result = enqueue.invoke(
                downloadService,
                imageInfo.id,
                msgIdTalker,
                resourceType,
                Integer.valueOf(-1),
                -1,
                callback,
                0,
                true);
        int code = result instanceof Number ? ((Number) result).intValue() : -1;
        if (code < 0 && code != -2) {
            forgetOriginalImageDownloadDispatch(recordId);
            throw new IllegalStateException("WeChat image download enqueue returned " + code);
        }
        log("original image download requested msgId=" + recordId
                + " resource=" + (resourceType == 1 ? "hd" : "large")
                + " result=" + code);
    }

    // WeChat only writes the voice audio blob to local storage lazily -- unlike images and
    // video, it does not appear to be auto-downloaded on receipt. Without this, resolveMediaFile
    // can find a real database row (voiceinfo/voiceinfo2) with a filename hint pointing at a
    // file that simply does not exist on disk yet, and no amount of client-side retrying can
    // conjure it. This mirrors the image original-download trigger, but for the WeChat internal
    // "VoiceMsgFeatureService.asyncDownloadVoice" API (canonical dg0.f / real yi0.f -> ej0.o.jj).
    private static void requestVoiceDownloadAsync(
            final long recordId, final Long msgSvrId, final String talker, final String fileNameHint) {
        if (recordId <= 0L || isBlank(talker) || isBlank(fileNameHint)
                || !markVoiceDownloadDispatched(recordId)) {
            return;
        }
        Runnable request = new Runnable() {
            @Override
            public void run() {
                try {
                    requestVoiceDownload(recordId, msgSvrId, talker, fileNameHint);
                } catch (Throwable t) {
                    forgetVoiceDownloadDispatch(recordId);
                    log("voice download request failed msgId=" + recordId
                            + " error=" + shortError(rootCause(t)));
                }
            }
        };
        if (Looper.myLooper() == Looper.getMainLooper()) {
            request.run();
            return;
        }
        if (!new Handler(Looper.getMainLooper()).post(request)) {
            forgetVoiceDownloadDispatch(recordId);
        }
    }

    private static void requestVoiceDownload(long recordId, Long msgSvrId, String talker, String fileNameHint)
            throws Exception {
        ClassLoader classLoader = runtimeClassLoader(WECHAT_CLASS_LOADER);
        if (classLoader == null) {
            throw new IllegalStateException("WeChat class loader is unavailable");
        }
        Class<?> serviceInterface = findVersionedClass(classLoader, "dg0.f");
        Object service = findMethod(findVersionedClass(classLoader, "i95.n0"), "c", Class.class)
                .invoke(null, serviceInterface);
        if (service == null) {
            throw new IllegalStateException("VoiceMsgFeatureService is not available");
        }
        Class<?> paramsClass = findVersionedClass(classLoader, "xi0.a");
        Constructor<?> paramsCtor = findConstructor(
                paramsClass, String.class, String.class, long.class, long.class, long.class);
        Object params = paramsCtor.newInstance(
                fileNameHint,
                talker,
                recordId,
                msgSvrId == null ? 0L : msgSvrId.longValue(),
                0L);
        Method download = findMethod(service.getClass(), "jj", paramsClass);
        download.invoke(service, params);
        log("voice download requested msgId=" + recordId + " fileName=" + fileNameHint);
    }

    private static boolean markVoiceDownloadDispatched(long recordId) {
        synchronized (VOICE_DOWNLOAD_DISPATCHED_IDS) {
            if (VOICE_DOWNLOAD_DISPATCHED_IDS.size() > 1024) {
                VOICE_DOWNLOAD_DISPATCHED_IDS.clear();
            }
            return VOICE_DOWNLOAD_DISPATCHED_IDS.add(recordId);
        }
    }

    private static void forgetVoiceDownloadDispatch(long recordId) {
        synchronized (VOICE_DOWNLOAD_DISPATCHED_IDS) {
            VOICE_DOWNLOAD_DISPATCHED_IDS.remove(recordId);
        }
    }

    // Same rationale as requestVoiceDownloadAsync, but for chat video: WeChat's
    // VideoDownloadFeatureService (canonical ri0.y1 / real si0.t) exposes an
    // asyncDownloadVideo-style "jj(e9 msgInfo)" that takes the message record directly,
    // no separate params object needed.
    private static void requestVideoDownloadAsync(final long recordId, final String talker) {
        if (recordId <= 0L || isBlank(talker) || !markVideoDownloadDispatched(recordId)) {
            return;
        }
        Runnable request = new Runnable() {
            @Override
            public void run() {
                try {
                    requestVideoDownload(recordId, talker);
                } catch (Throwable t) {
                    forgetVideoDownloadDispatch(recordId);
                    log("video download request failed msgId=" + recordId
                            + " error=" + shortError(rootCause(t)));
                }
            }
        };
        if (Looper.myLooper() == Looper.getMainLooper()) {
            request.run();
            return;
        }
        if (!new Handler(Looper.getMainLooper()).post(request)) {
            forgetVideoDownloadDispatch(recordId);
        }
    }

    private static void requestVideoDownload(long recordId, String talker) throws Exception {
        ClassLoader classLoader = runtimeClassLoader(WECHAT_CLASS_LOADER);
        if (classLoader == null) {
            throw new IllegalStateException("WeChat class loader is unavailable");
        }
        WeChatVersionSymbols.Profile symbols = weChatSymbols(classLoader);
        Class<?> messageStorageClass = findVersionedClass(classLoader, "qt0.k0");
        Class<?> messageClass = findVersionedClass(classLoader, "com.tencent.mm.storage.f9");
        Object message = findMethod(messageStorageClass, symbols.methods.imageMessageLookup, String.class, long.class)
                .invoke(null, talker, recordId);
        if (message == null) {
            throw new IllegalStateException("video message record was not found");
        }
        Class<?> serviceInterface = findVersionedClass(classLoader, "ri0.y1");
        Object service = findMethod(findVersionedClass(classLoader, "i95.n0"), "c", Class.class)
                .invoke(null, serviceInterface);
        if (service == null) {
            throw new IllegalStateException("VideoDownloadFeatureService is not available");
        }
        Method download = findMethod(service.getClass(), "jj", messageClass);
        Object result = download.invoke(service, message);
        log("video download requested msgId=" + recordId + " result=" + result);
    }

    private static boolean markVideoDownloadDispatched(long recordId) {
        synchronized (VIDEO_DOWNLOAD_DISPATCHED_IDS) {
            if (VIDEO_DOWNLOAD_DISPATCHED_IDS.size() > 1024) {
                VIDEO_DOWNLOAD_DISPATCHED_IDS.clear();
            }
            return VIDEO_DOWNLOAD_DISPATCHED_IDS.add(recordId);
        }
    }

    private static void forgetVideoDownloadDispatch(long recordId) {
        synchronized (VIDEO_DOWNLOAD_DISPATCHED_IDS) {
            VIDEO_DOWNLOAD_DISPATCHED_IDS.remove(recordId);
        }
    }

    private static ImageInfoRecord imageInfoFromWechatStorage(
            ClassLoader classLoader,
            long recordId,
            Long msgSvrId,
            String talker) throws Exception {
        Object storage = wechatImageStorage(classLoader);
        if (storage == null) {
            return null;
        }
        // These two lookups (by msgSvrId vs. by local recordId) were hardcoded
        // as "K2"/"A2" without ever going through WeChatVersionSymbols, and
        // that pair could not even be found on the 8.0.77 class this was
        // supposedly written against -- this path was never actually
        // exercised before. WeChat 8.0.78's class has exactly one pair of
        // same-signature (String,long) methods that plausibly match ("F3" and
        // "x3"); try both orderings per lookup so this self-corrects if the
        // guessed pairing is backwards.
        Object model = null;
        if (msgSvrId != null && msgSvrId > 0L) {
            model = invokeFirstWorking(storage, new String[]{"K2", "F3", "x3"}, talker, msgSvrId.longValue());
        }
        ImageInfoRecord record = imageInfoFromWechatModel(model);
        if ((record == null || record.id <= 0L) && recordId > 0L) {
            model = invokeFirstWorking(storage, new String[]{"A2", "x3", "F3"}, talker, recordId);
            record = imageInfoFromWechatModel(model);
        }
        return record;
    }

    private static Object invokeFirstWorking(Object target, String[] methodNames, String arg0, long arg1) {
        for (String name : methodNames) {
            try {
                return findMethod(target.getClass(), name, String.class, long.class).invoke(target, arg0, arg1);
            } catch (Throwable ignored) {
            }
        }
        return null;
    }

    private static ImageInfoRecord imageInfoByIdFromWechatStorage(ClassLoader classLoader, long imageInfoId)
            throws Exception {
        if (imageInfoId <= 0L) {
            return null;
        }
        Object storage = wechatImageStorage(classLoader);
        if (storage == null) {
            return null;
        }
        Object model = findMethod(storage.getClass(), "U1", Long.class)
                .invoke(storage, Long.valueOf(imageInfoId));
        return imageInfoFromWechatModel(model);
    }

    private static Object wechatImageStorage(ClassLoader classLoader) throws Exception {
        WeChatVersionSymbols.Profile symbols = weChatSymbols(classLoader);
        Class<?> serviceManagerClass = findVersionedClass(classLoader, "i95.n0");
        Class<?> imageFeatureClass = findVersionedClass(classLoader, "h70.y");
        Object imageFeature = findMethod(serviceManagerClass, "c", Class.class)
                .invoke(null, imageFeatureClass);
        return imageFeature == null
                ? null
                : findNoArgMethod(imageFeature.getClass(), symbols.methods.imageStorage).invoke(imageFeature);
    }

    private static ImageInfoRecord imageInfoFromWechatModel(Object model) throws Exception {
        if (model == null) {
            return null;
        }
        ImageInfoRecord record = new ImageInfoRecord();
        record.id = getLongField(model, "a", "f386887a");
        record.hdId = getLongField(model, "q", "f386903q");
        record.offset = (int) getLongField(model, "c", "f386889c");
        record.totalLen = (int) getLongField(model, "d", "f386890d");
        record.bigPath = getStringField(model, "e", "f386891e");
        record.midPath = getStringField(model, "f", "f386892f");
        return record;
    }

    private static InvocationHandler originalImageDownloadCallback(final long recordId) {
        return new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) {
                String name = method.getName();
                if ("hashCode".equals(name)) {
                    return System.identityHashCode(proxy);
                }
                if ("equals".equals(name)) {
                    return args != null && args.length == 1 && proxy == args[0];
                }
                if ("toString".equals(name)) {
                    return "ObservatoryOriginalImageCallback(" + recordId + ")";
                }
                if ("f".equals(name) || "c".equals(name)) {
                    int errorType = intArgument(args, 6);
                    int errorCode = intArgument(args, 7);
                    if (errorType != 0 || errorCode != 0) {
                        forgetOriginalImageDownloadDispatch(recordId);
                    } else {
                        captureOriginalImageDownloadPath(recordId, args);
                    }
                    log("original image download completed msgId=" + recordId
                            + " errorType=" + errorType
                            + " errorCode=" + errorCode);
                } else if ("n".equals(name) || "l".equals(name)) {
                    forgetOriginalImageDownloadDispatch(recordId);
                }
                return defaultValue(method.getReturnType());
            }
        };
    }

    private static void captureOriginalImageDownloadPath(long recordId, Object[] args) {
        try {
            Object scene = args != null && args.length > 9 ? args[9] : null;
            if (scene == null) {
                logOriginalImageDiagnostic(recordId, "download callback scene is missing");
                return;
            }
            String vfsPath = getStringField(scene, "y", "f387049y");
            if (isBlank(vfsPath)) {
                logOriginalImageDiagnostic(recordId, "download callback path is empty");
                return;
            }
            String localPath = materializeOriginalImagePath(recordId, scene.getClass().getClassLoader(), vfsPath);
            if (isBlank(localPath)) {
                logOriginalImageDiagnostic(recordId, "downloaded image could not be materialized");
                return;
            }
            rememberOriginalImageDownloadedPath(recordId, localPath);
            File file = new File(localPath);
            log("original image callback path ready msgId=" + recordId
                    + " exists=" + file.isFile()
                    + " size=" + (file.isFile() ? file.length() : 0L));
        } catch (Throwable t) {
            logOriginalImageDiagnostic(recordId, "download callback path failed: " + shortError(rootCause(t)));
        }
    }

    private static String materializeOriginalImagePath(
            long recordId,
            ClassLoader classLoader,
            String vfsPath) throws Exception {
        Class<?> vfsClass = findVersionedClass(classLoader, "com.tencent.mm.vfs.w6");
        Object rawPath = findMethod(vfsClass, "i", String.class, boolean.class)
                .invoke(null, vfsPath, false);
        String directPath = rawPath == null ? "" : String.valueOf(rawPath);
        if (!isBlank(directPath)) {
            File direct = new File(directPath);
            if (direct.isFile() && direct.length() > 0L) {
                return direct.getAbsolutePath();
            }
        }

        Context context = bridgeContext();
        File cacheRoot = context == null ? null : context.getCacheDir();
        if (cacheRoot == null) {
            throw new IOException("inbound image cache is unavailable");
        }
        File dir = new File(cacheRoot, "wechat-observatory-inbound");
        if (!dir.isDirectory() && !dir.mkdirs()) {
            throw new IOException("create inbound image cache failed");
        }
        File target = new File(dir, "image-" + recordId + ".img");
        Object opened = findMethod(vfsClass, "B", String.class, boolean.class)
                .invoke(null, vfsPath, false);
        if (!(opened instanceof java.io.RandomAccessFile)) {
            throw new IOException("WeChat VFS did not return a readable image file");
        }
        try (java.io.RandomAccessFile input = (java.io.RandomAccessFile) opened;
             FileOutputStream output = new FileOutputStream(target, false)) {
            long length = input.length();
            if (length <= 0L || length > OUTBOX_MEDIA_DOWNLOAD_LIMIT_BYTES) {
                throw new IOException("downloaded image size is invalid: " + length);
            }
            input.seek(0L);
            byte[] buffer = new byte[8192];
            int read;
            long total = 0L;
            while ((read = input.read(buffer)) != -1) {
                total += read;
                if (total > OUTBOX_MEDIA_DOWNLOAD_LIMIT_BYTES) {
                    throw new IOException("downloaded image exceeds cache limit");
                }
                output.write(buffer, 0, read);
            }
        }
        return target.isFile() && target.length() > 0L ? target.getAbsolutePath() : "";
    }

    private static void rememberOriginalImageDownloadedPath(long recordId, String path) {
        if (recordId <= 0L || isBlank(path)) {
            return;
        }
        synchronized (ORIGINAL_IMAGE_DOWNLOADED_PATHS) {
            if (ORIGINAL_IMAGE_DOWNLOADED_PATHS.size() >= 256) {
                ORIGINAL_IMAGE_DOWNLOADED_PATHS.clear();
            }
            ORIGINAL_IMAGE_DOWNLOADED_PATHS.put(Long.valueOf(recordId), path);
        }
    }

    private static String originalImageDownloadedPath(Long recordId) {
        if (recordId == null || recordId <= 0L) {
            return "";
        }
        synchronized (ORIGINAL_IMAGE_DOWNLOADED_PATHS) {
            String path = ORIGINAL_IMAGE_DOWNLOADED_PATHS.get(recordId);
            return path == null ? "" : path;
        }
    }

    private static void logOriginalImageDiagnostic(long recordId, String message) {
        synchronized (ORIGINAL_IMAGE_DIAGNOSTIC_REPORTED_IDS) {
            if (ORIGINAL_IMAGE_DIAGNOSTIC_REPORTED_IDS.size() >= 1024) {
                ORIGINAL_IMAGE_DIAGNOSTIC_REPORTED_IDS.clear();
            }
            if (!ORIGINAL_IMAGE_DIAGNOSTIC_REPORTED_IDS.add(Long.valueOf(recordId))) {
                return;
            }
        }
        log("original image diagnostic msgId=" + recordId + " error=" + message);
    }

    private static int intArgument(Object[] args, int index) {
        if (args == null || index < 0 || index >= args.length || !(args[index] instanceof Number)) {
            return 0;
        }
        return ((Number) args[index]).intValue();
    }

    private static boolean markOriginalImageDownloadDispatched(long recordId) {
        synchronized (ORIGINAL_IMAGE_DOWNLOAD_DISPATCHED_IDS) {
            if (ORIGINAL_IMAGE_DOWNLOAD_DISPATCHED_IDS.size() > 1024) {
                ORIGINAL_IMAGE_DOWNLOAD_DISPATCHED_IDS.clear();
            }
            return ORIGINAL_IMAGE_DOWNLOAD_DISPATCHED_IDS.add(recordId);
        }
    }

    private static void forgetOriginalImageDownloadDispatch(long recordId) {
        synchronized (ORIGINAL_IMAGE_DOWNLOAD_DISPATCHED_IDS) {
            ORIGINAL_IMAGE_DOWNLOAD_DISPATCHED_IDS.remove(recordId);
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(Math.max(0L, millis));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static boolean markMediaRetryScheduled(long chatRecordId) {
        if (chatRecordId <= 0) {
            return false;
        }
        synchronized (MEDIA_RETRY_SCHEDULED_IDS) {
            if (MEDIA_RETRY_SCHEDULED_IDS.size() > 1024) {
                MEDIA_RETRY_SCHEDULED_IDS.clear();
            }
            if (MEDIA_RETRY_SCHEDULED_IDS.contains(chatRecordId)) {
                return false;
            }
            MEDIA_RETRY_SCHEDULED_IDS.add(chatRecordId);
            return true;
        }
    }

    private static void rememberMediaUpload(MessagePayload payload) {
        if (payload == null || payload.chatRecordId <= 0 || isBlank(payload.mediaBase64)) {
            return;
        }
        synchronized (MEDIA_UPLOAD_REPORTED_IDS) {
            if (MEDIA_UPLOAD_REPORTED_IDS.size() > 1024) {
                MEDIA_UPLOAD_REPORTED_IDS.clear();
            }
            MEDIA_UPLOAD_REPORTED_IDS.add(payload.chatRecordId);
        }
    }

    private static boolean hasRememberedMediaUpload(long chatRecordId) {
        if (chatRecordId <= 0) {
            return false;
        }
        synchronized (MEDIA_UPLOAD_REPORTED_IDS) {
            return MEDIA_UPLOAD_REPORTED_IDS.contains(chatRecordId);
        }
    }

    private static void logMediaRetryFailure(long recordId, int type, int attempt, Throwable t) {
        long now = System.currentTimeMillis();
        if (now - LAST_MEDIA_RETRY_FAIL_LOG_AT < 30000L) {
            return;
        }
        LAST_MEDIA_RETRY_FAIL_LOG_AT = now;
        log("media retry failed type=" + type
                + " msgId=" + recordId
                + " attempt=" + attempt
                + " error=" + shortError(rootCause(t)));
    }

    private static Throwable rootCause(Throwable t) {
        Throwable current = t;
        int depth = 0;
        while (current != null && current.getCause() != null && depth < 8) {
            current = current.getCause();
            depth++;
        }
        return current == null ? t : current;
    }

    private static String detailedErrorChain(Throwable t) {
        if (t == null) {
            return "";
        }
        StringBuilder out = new StringBuilder(192);
        Throwable current = t;
        int depth = 0;
        while (current != null && depth < 8) {
            if (depth > 0) {
                out.append(" <- ");
            }
            out.append(current.getClass().getSimpleName());
            String message = shortError(current);
            if (!isBlank(message) && !message.equals(current.getClass().getName())) {
                out.append(": ").append(message);
            }
            Throwable next = current.getCause();
            if (next == current) {
                break;
            }
            current = next;
            depth++;
        }
        String text = out.toString();
        return text.length() > 480 ? text.substring(0, 480) + "..." : text;
    }

    private static boolean shouldReportMessage(String talker, String content, int type) {
        if (isBlank(talker)) {
            return false;
        }
        String normalizedTalker = talker.trim().toLowerCase(Locale.US);
        if (normalizedTalker.startsWith("gh_") || isSystemContact(normalizedTalker)) {
            return false;
        }
        if (type == 1) {
            if (isBlank(content)) {
                return false;
            }
            String text = normalizeMessageText(talker, content);
            return !looksLikeXmlPayload(text);
        }
        return type > 0;
    }

    private static MessagePayload buildMessagePayload(BridgeConfig config, String talker, String content, Integer isSend, Long msgId, Long createTime, int type, String mediaHint) {
        return buildMessagePayload(
                config, talker, content, isSend, msgId, createTime, type, mediaHint, "");
    }

    private static MessagePayload buildMessagePayload(
            BridgeConfig config,
            String talker,
            String content,
            Integer isSend,
            Long msgId,
            Long createTime,
            int type,
            String mediaHint,
            String msgSource) {
        MessagePayload payload = new MessagePayload();
        String normalizedTalker = talker == null ? "" : talker.trim();
        boolean chatroom = isChatroomTalker(normalizedTalker);
        String text = displayMessageText(normalizedTalker, content, type);
        String normalizedContent = normalizeMessageContent(normalizedTalker, content, type);
        payload.id = msgId == null || msgId <= 0 ? "" : String.valueOf(msgId);
        payload.eventId = msgId == null ? 0L : msgId;
        payload.chatRecordId = msgId == null ? 0L : msgId;
        payload.apiKey = config.apiKey;
        payload.device = config.device;
        payload.chatId = normalizedTalker;
        payload.chatKind = chatroom ? "room" : "direct";
        payload.roomId = chatroom ? normalizedTalker : "";
        payload.text = text;
        payload.messageType = type;
        payload.rawXml = rawXmlPayload(normalizedContent, type);
        payload.createTime = normalizeCreateTime(createTime);
        attachMedia(config, payload, type, mediaHint, payload.createTime, normalizedTalker, content);

        boolean sent = isSend != null && isSend == 1;
        if (sent) {
            payload.direction = "sent";
            payload.from = config.selfWxid;
            payload.to = normalizedTalker;
            payload.sender = chatroom ? config.selfWxid : "";
        } else {
            payload.direction = "recv";
            payload.from = normalizedTalker;
            payload.to = config.selfWxid;
            payload.sender = chatroom ? extractChatroomSender(content) : "";
            payload.atSelf = chatroom && InboundMentionPolicy.mentionsSelf(msgSource, config.selfWxid);
        }
        return payload;
    }

    private static void attachMedia(
            BridgeConfig config,
            MessagePayload payload,
            int type,
            String mediaHint,
            long createTime,
            String talker,
            String content) {
        String kind = mediaKindForMessageType(type);
        if (isBlank(kind)) {
            return;
        }
        payload.mediaKind = kind;
        if (!config.mediaUploadEnabled) {
            return;
        }
        String emojiMd5 = type == 47 ? emojiMd5FromContent(talker, content) : "";
        File file = resolveMediaFile(type, mediaHint, createTime, emojiMd5);
        if (file == null || !file.isFile()) {
            if (type == 34 || type == 43 || type == 47 || type == 62) {
                log("media file not found type=" + type
                        + " msgId=" + payload.chatRecordId
                        + " hintPresent=" + !isBlank(mediaHint));
            }
            return;
        }
        long length = file.length();
        if (length <= 0 || length > config.mediaUploadLimitBytes) {
            log("skip media upload type=" + type + " size=" + length + " path=" + file.getAbsolutePath());
            return;
        }
        try {
            byte[] bytes = readFileBytes(file, config.mediaUploadLimitBytes);
            String mime = detectMediaMime(type, file, bytes);
            payload.mediaMime = mime;
            payload.mediaName = mediaUploadName(file, mime, payload.id);
            payload.mediaSize = bytes.length;
            payload.mediaBase64 = Base64.encodeToString(bytes, Base64.NO_WRAP);
        } catch (Throwable t) {
            log("read media failed type=" + type + " hint=" + mediaHint + " error=" + shortError(t));
        }
    }

    private static String mediaKindForMessageType(int type) {
        switch (type) {
            case 3:
                return "image";
            case 34:
                return "voice";
            case 43:
            case 62:
                return "video";
            case 47:
                return "emoji";
            case 48:
                return "location";
            case 49:
            case MESSAGE_TYPE_FILE_TRANSFER:
                return "file";
            default:
                return "";
        }
    }

    private static String resolveMediaHint(Object db, int type, Long msgId, Long msgSvrId, String mediaHint) {
        if (type == 3 && supportsOriginalImageDownload()) {
            String downloadedPath = originalImageDownloadedPath(msgId);
            if (!isBlank(downloadedPath)) {
                return downloadedPath;
            }
            try {
                String imageHint = imageMediaHintFromWechatStorage(db, msgId, msgSvrId);
                if (!isBlank(imageHint)) {
                    return imageHint;
                }
            } catch (Throwable t) {
                logOriginalImageDiagnostic(
                        msgId == null ? 0L : msgId.longValue(),
                        "storage path failed: " + shortError(rootCause(t)));
            }
        }
        if (type == 3 && db != null) {
            String imageHint = imageMediaHintFromInfoTable(db, msgId, msgSvrId);
            if (!isBlank(imageHint)) {
                return imageHint;
            }
        }
        if (type == 34 && db != null) {
            String voiceHint = voiceMediaHintFromInfoTable(db, msgId, msgSvrId);
            if (!isBlank(voiceHint)) {
                return voiceHint;
            }
        }
        if ((type == 43 || type == 62) && db != null) {
            String videoHint = videoMediaHintFromInfoTable(db, msgId, msgSvrId);
            if (!isBlank(videoHint)) {
                return videoHint;
            }
        }
        return mediaHint;
    }

    private static String imageMediaHintFromInfoTable(Object db, Long msgId, Long msgSvrId) {
        ImageInfoRecord base = queryImageInfoRecord(db, msgId, msgSvrId);
        if (base == null) {
            return "";
        }
        ImageInfoRecord selected = base;
        if (base.hdId > 0L) {
            selected = queryImageInfoRecordById(db, base.hdId);
            if (selected == null) {
                return "";
            }
        }
        return completeOriginalImageHint(selected);
    }

    private static String wechatImageFullPath(
            ClassLoader classLoader,
            String talker,
            long msgId,
            int resourceType,
            String logicalPath) throws Exception {
        if (isBlank(talker) || msgId <= 0L || isBlank(logicalPath)) {
            return "";
        }
        WeChatVersionSymbols.Profile symbols = weChatSymbols(classLoader);
        Class<?> messageStorageClass = findVersionedClass(classLoader, "qt0.k0");
        Class<?> messageClass = findVersionedClass(classLoader, "com.tencent.mm.storage.f9");
        Object message = findMethod(messageStorageClass, symbols.methods.imageMessageLookup, String.class, long.class)
                .invoke(null, talker, msgId);
        if (message == null) {
            return "";
        }

        Class<?> serviceManagerClass = findVersionedClass(classLoader, "i95.n0");
        Class<?> pathServiceClass = findVersionedClass(classLoader, "tg3.u0");
        Object pathService = findMethod(serviceManagerClass, "c", Class.class)
                .invoke(null, pathServiceClass);
        Class<?> imageTypeClass = findVersionedClass(classLoader, "bm5.f0");
        Object imageType = findFieldAny(
                imageTypeClass,
                resourceType == 1 ? "g" : "h",
                resourceType == 1 ? "f320138g" : "f320139h").get(null);
        Object resolved;
        if (resourceType == 1) {
            resolved = findMethod(
                    pathService.getClass(),
                    symbols.methods.imageHdPath,
                    messageClass,
                    imageTypeClass,
                    String.class,
                    String.class,
                    String.class).invoke(pathService, message, imageType, logicalPath, "", "");
        } else {
            resolved = findMethod(
                    pathService.getClass(),
                    symbols.methods.imageLargePath,
                    messageClass,
                    imageTypeClass,
                    String.class,
                    String.class,
                    String.class,
                    boolean.class,
                    boolean.class).invoke(pathService, message, imageType, logicalPath, "", "", true, false);
        }
        String vfsPath = resolved == null ? "" : String.valueOf(resolved);
        if (isBlank(vfsPath)) {
            return "";
        }
        Object rawPath = findMethod(
                findVersionedClass(classLoader, "com.tencent.mm.vfs.w6"),
                "i",
                String.class,
                boolean.class).invoke(null, vfsPath, false);
        String path = rawPath == null ? vfsPath : String.valueOf(rawPath);
        File file = new File(path);
        log("original image path resolved msgId=" + msgId
                + " resource=" + (resourceType == 1 ? "hd" : "large")
                + " exists=" + file.isFile()
                + " size=" + (file.isFile() ? file.length() : 0L));
        return path;
    }

    private static String imageMediaHintFromWechatStorage(Object db, Long msgId, Long msgSvrId)
            throws Exception {
        ClassLoader classLoader = runtimeClassLoader(WECHAT_CLASS_LOADER);
        if (classLoader == null) {
            return "";
        }
        String talker = imageTalkerForMessage(db, msgId);
        if (isBlank(talker)) {
            return "";
        }
        ImageInfoRecord base = imageInfoFromWechatStorage(
                classLoader,
                msgId == null ? 0L : msgId.longValue(),
                msgSvrId,
                talker);
        if (base == null) {
            return "";
        }
        ImageInfoRecord selected = base;
        if (base.hdId > 0L) {
            selected = imageInfoByIdFromWechatStorage(classLoader, base.hdId);
            if (selected == null) {
                return "";
            }
        }
        String logicalPath = completeOriginalImageHint(selected);
        if (isBlank(logicalPath)) {
            return "";
        }
        String fullPath = wechatImageFullPath(
                classLoader,
                talker,
                msgId == null ? 0L : msgId.longValue(),
                base.hdId > 0L ? 1 : 0,
                logicalPath);
        return isBlank(fullPath) ? logicalPath : fullPath;
    }

    private static String completeOriginalImageHint(ImageInfoRecord record) {
        if (record == null || !record.isComplete()) {
            return "";
        }
        if (!isBlank(record.bigPath) && !InboundImagePolicy.isThumbnailPath(record.bigPath)) {
            return record.bigPath;
        }
        if (!isBlank(record.midPath) && !InboundImagePolicy.isThumbnailPath(record.midPath)) {
            return record.midPath;
        }
        return "";
    }

    private static String imageTalkerForMessage(Object db, Long msgId) {
        if (db == null || msgId == null || msgId <= 0L) {
            return "";
        }
        Object cursor = null;
        try {
            cursor = rawQuery(
                    db,
                    "SELECT talker FROM message WHERE msgId=? LIMIT 1",
                    new String[]{String.valueOf(msgId)});
            if (cursor == null || !Boolean.TRUE.equals(findNoArgMethod(cursor.getClass(), "moveToFirst").invoke(cursor))) {
                return "";
            }
            return stringColumn(cursor, 0);
        } catch (Throwable ignored) {
            return "";
        } finally {
            closeQuietly(cursor);
        }
    }

    private static String messageSourceForMessage(Object db, long msgId) {
        if (db == null || msgId <= 0L) {
            return "";
        }
        Object cursor = null;
        try {
            cursor = rawQuery(
                    db,
                    "SELECT COALESCE(msgSource,'') FROM message WHERE msgId=? LIMIT 1",
                    new String[]{String.valueOf(msgId)});
            if (cursor == null
                    || !Boolean.TRUE.equals(findNoArgMethod(cursor.getClass(), "moveToFirst").invoke(cursor))) {
                return "";
            }
            return stringColumn(cursor, 0);
        } catch (Throwable ignored) {
            return "";
        } finally {
            closeQuietly(cursor);
        }
    }

    private static ImageInfoRecord queryImageInfoRecord(Object db, Long msgId, Long msgSvrId) {
        if (db == null) {
            return null;
        }
        if (msgSvrId != null && msgSvrId > 0L) {
            ImageInfoRecord byServerId = queryImageInfoRecord(
                    db,
                    "msgSvrId=? ORDER BY CASE WHEN reserved1>0 THEN 0 ELSE 1 END, id ASC LIMIT 1",
                    new String[]{String.valueOf(msgSvrId)});
            if (byServerId != null && byServerId.id > 0L) {
                return byServerId;
            }
        }
        if (msgId != null && msgId > 0L) {
            return queryImageInfoRecord(
                    db,
                    "msglocalid=? ORDER BY CASE WHEN reserved1>0 THEN 0 ELSE 1 END, id DESC LIMIT 1",
                    new String[]{String.valueOf(msgId)});
        }
        return null;
    }

    private static ImageInfoRecord queryImageInfoRecordById(Object db, long imageInfoId) {
        if (db == null || imageInfoId <= 0L) {
            return null;
        }
        return queryImageInfoRecord(db, "id=? LIMIT 1", new String[]{String.valueOf(imageInfoId)});
    }

    private static ImageInfoRecord queryImageInfoRecord(Object db, String whereClause, String[] args) {
        Object cursor = null;
        try {
            cursor = rawQuery(
                    db,
                    "SELECT id,reserved1,COALESCE(bigImgPath,''),COALESCE(midImgPath,''),"
                            + "COALESCE(totalLen,0),COALESCE(offset,0) "
                            + "FROM ImgInfo2 WHERE " + whereClause,
                    args);
            if (cursor == null) {
                return null;
            }
            Method moveToFirst = findNoArgMethod(cursor.getClass(), "moveToFirst");
            if (!Boolean.TRUE.equals(moveToFirst.invoke(cursor))) {
                return null;
            }
            ImageInfoRecord record = new ImageInfoRecord();
            record.id = longColumn(cursor, 0);
            record.hdId = longColumn(cursor, 1);
            record.bigPath = stringColumn(cursor, 2);
            record.midPath = stringColumn(cursor, 3);
            record.totalLen = intColumn(cursor, 4);
            record.offset = intColumn(cursor, 5);
            return record;
        } catch (Throwable ignored) {
            return null;
        } finally {
            closeQuietly(cursor);
        }
    }

    private static String videoMediaHintFromInfoTable(Object db, Long msgId, Long msgSvrId) {
        List<String> columns = tableColumns(db, "videoinfo2");
        if (columns.isEmpty()) {
            return "";
        }
        List<String> valueColumns = existingColumns(columns, new String[]{
                "filename", "fileName", "videoPath", "videopath", "fullPath", "fullpath", "path", "thumbPath", "thumbpath"
        });
        if (valueColumns.isEmpty()) {
            return "";
        }
        List<String> idColumns = existingColumns(columns, new String[]{
                "msglocalid", "msgLocalId", "msgid", "msgId", "msgsvrid", "msgSvrId", "msgSvrID", "svrid", "svrId"
        });
        if (idColumns.isEmpty()) {
            return "";
        }
        List<Long> ids = new ArrayList<>();
        addPositiveId(ids, msgId);
        addPositiveId(ids, msgSvrId);
        for (String idColumn : idColumns) {
            for (Long id : ids) {
                String hint = queryVideoInfoHint(db, idColumn, id.longValue(), valueColumns);
                if (!isBlank(hint)) {
                    return hint;
                }
            }
        }
        return "";
    }

    private static String voiceMediaHintFromInfoTable(Object db, Long msgId, Long msgSvrId) {
        String knownHint = voiceMediaHintFromKnownColumns(db, msgId, msgSvrId);
        if (!isBlank(knownHint)) {
            return knownHint;
        }
        String[] tables = new String[]{"voiceinfo", "voiceinfo2"};
        for (String table : tables) {
            List<String> columns = tableColumns(db, table);
            if (columns.isEmpty()) {
                continue;
            }
            List<String> valueColumns = existingColumns(columns, new String[]{
                    "FileName", "filename", "fileName", "file_name", "voicePath", "voicepath",
                    "fullPath", "fullpath", "path", "localPath", "filePath", "filepath",
                    "amrPath", "silkPath"
            });
            if (valueColumns.isEmpty()) {
                continue;
            }
            List<String> idColumns = existingColumns(columns, new String[]{
                    "MsgLocalId", "msgLocalId", "msglocalid",
                    "MsgId", "msgId", "msgid", "msgSvrId", "MsgSvrId", "MsgSvrID",
                    "msgsvrid", "svrId", "svrid"
            });
            if (idColumns.isEmpty()) {
                continue;
            }
            List<Long> ids = new ArrayList<>();
            addPositiveId(ids, msgId);
            addPositiveId(ids, msgSvrId);
            for (String idColumn : idColumns) {
                for (Long id : ids) {
                    String hint = queryMediaInfoHint(db, table, idColumn, id.longValue(), valueColumns);
                    if (!isBlank(hint)) {
                        return hint;
                    }
                }
            }
        }
        return "";
    }

    private static String voiceMediaHintFromKnownColumns(Object db, Long msgId, Long msgSvrId) {
        List<Long> ids = new ArrayList<>();
        addPositiveId(ids, msgId);
        addPositiveId(ids, msgSvrId);
        String[] tables = new String[]{"voiceinfo", "voiceinfo2"};
        String[] idColumns = new String[]{"MsgLocalId", "MsgId", "MsgSvrId"};
        for (String table : tables) {
            for (String idColumn : idColumns) {
                for (Long id : ids) {
                    String hint = queryMediaInfoHint(
                            db,
                            table,
                            idColumn,
                            id.longValue(),
                            Collections.singletonList("FileName"));
                    if (!isBlank(hint)) {
                        return hint;
                    }
                }
            }
        }
        return "";
    }

    private static void addPositiveId(List<Long> ids, Long value) {
        if (value == null || value <= 0L) {
            return;
        }
        for (Long existing : ids) {
            if (existing.longValue() == value.longValue()) {
                return;
            }
        }
        ids.add(value);
    }

    private static String queryVideoInfoHint(Object db, String idColumn, long id, List<String> valueColumns) {
        return queryMediaInfoHint(db, "videoinfo2", idColumn, id, valueColumns);
    }

    private static String queryMediaInfoHint(Object db, String table, String idColumn, long id, List<String> valueColumns) {
        if (id <= 0L) {
            return "";
        }
        String sql = "SELECT " + joinIdentifiers(valueColumns)
                + " FROM " + sqlIdentifier(table)
                + " WHERE " + sqlIdentifier(idColumn) + "=? LIMIT 1";
        Object cursor = null;
        try {
            cursor = rawQuery(db, sql, new String[]{String.valueOf(id)});
            if (cursor == null) {
                return "";
            }
            Method moveToFirst = findNoArgMethod(cursor.getClass(), "moveToFirst");
            if (!Boolean.TRUE.equals(moveToFirst.invoke(cursor))) {
                return "";
            }
            for (int i = 0; i < valueColumns.size(); i++) {
                String value = stringColumn(cursor, i);
                if (!isBlank(value)) {
                    return value;
                }
            }
            return "";
        } catch (Throwable ignored) {
            return "";
        } finally {
            closeQuietly(cursor);
        }
    }

    private static List<String> tableColumns(Object db, String table) {
        List<String> columns = new ArrayList<>();
        Object cursor = null;
        try {
            cursor = rawQuery(db, "PRAGMA table_info(" + sqlIdentifier(table) + ")", new String[]{});
            if (cursor == null) {
                return columns;
            }
            Method moveToNext = findNoArgMethod(cursor.getClass(), "moveToNext");
            while (Boolean.TRUE.equals(moveToNext.invoke(cursor))) {
                addMediaCandidate(columns, stringColumn(cursor, 1));
            }
            return columns;
        } catch (Throwable ignored) {
            return columns;
        } finally {
            closeQuietly(cursor);
        }
    }

    private static List<String> existingColumns(List<String> columns, String[] candidates) {
        List<String> existing = new ArrayList<>();
        if (columns == null || candidates == null) {
            return existing;
        }
        for (String candidate : candidates) {
            String column = existingColumn(columns, candidate);
            if (!isBlank(column)) {
                addMediaCandidate(existing, column);
            }
        }
        return existing;
    }

    private static String existingColumn(List<String> columns, String candidate) {
        if (isBlank(candidate)) {
            return "";
        }
        for (String column : columns) {
            if (!isBlank(column) && column.equalsIgnoreCase(candidate)) {
                return column;
            }
        }
        return "";
    }

    private static String joinIdentifiers(List<String> identifiers) {
        StringBuilder builder = new StringBuilder();
        for (String identifier : identifiers) {
            if (builder.length() > 0) {
                builder.append(',');
            }
            builder.append(sqlIdentifier(identifier));
        }
        return builder.toString();
    }

    private static String sqlIdentifier(String name) {
        if (name == null) {
            return "``";
        }
        return "`" + name.replace("`", "``") + "`";
    }

    private static File resolveMediaFile(int type, String mediaHint, long createTime, String emojiMd5) {
        List<String> names = mediaCandidateNames(type, mediaHint);
        if (type == 47) {
            addEmojiCandidateVariants(names, emojiMd5);
        }
        if (names.isEmpty() && type != 34) {
            return null;
        }
        File direct = directMediaFile(mediaHint);
        if (direct != null && (type != 3 || !InboundImagePolicy.isThumbnailPath(direct.getName()))) {
            return direct;
        }
        Context context = APP_CONTEXT;
        if (context == null) {
            Object app = currentApplication();
            if (app instanceof Context) {
                context = (Context) app;
            }
        }
        if (context == null) {
            return null;
        }
        File appRoot = context.getFilesDir() == null ? null : context.getFilesDir().getParentFile();
        if (appRoot == null) {
            return null;
        }
        String[] roots = mediaSearchRoots(type);
        File microMsgRoot = new File(appRoot, "MicroMsg");
        if (!names.isEmpty()) {
            File found = findMediaInProfileRoots(microMsgRoot, roots, names);
            if (found != null) {
                if (type == 47) {
                    log("emoji media selected file=" + found.getName() + " size=" + found.length());
                }
                return found;
            }
        }
        if (type == 47) {
            File found = findEmojiMediaOutsideProfile(appRoot, names);
            if (found != null) {
                log("emoji media selected file=" + found.getName() + " size=" + found.length());
                return found;
            }
            logEmojiInfoDiagnostic(emojiMd5);
        }
        if (type == 34) {
            // voice2/ is sharded two levels deep by MD5(hint)[0:2]/[2:4] (confirmed against
            // real downloaded files, e.g. hint "amr_<hash>" -> voice2/96/99/msg_amr_<hash>.amr)
            // and holds tens of thousands of files across 256 top-level shards. The generic
            // findMediaInProfileRoots search below is breadth-limited (visited cap 6000) so it
            // routinely gives up long before reaching the correct shard for a hash-based
            // filename -- that cap exists to bound a small, un-sharded directory, not this one.
            // Since the exact hint is already known, compute the shard path directly instead.
            File sharded = voiceShardedFile(microMsgRoot, mediaHint);
            if (sharded != null) {
                return sharded;
            }
            // Never fall back to a fuzzy/global scan for voice: if the sharded lookup and the
            // exact-name profile search both miss, reporting no media is safer than leaking
            // audio across chats or users.
            logVoiceMediaMissDiagnostic(microMsgRoot, names);
            return null;
        }
        if (type == 43 || type == 62) {
            return findMediaInProfileRoots(new File(appRoot, "cache"), videoCacheSearchRoots(), names);
        }
        return null;
    }

    private static File voiceShardedFile(File microMsgRoot, String hint) {
        if (isBlank(hint) || microMsgRoot == null) {
            return null;
        }
        String base = stripKnownMediaExtension(hint.trim());
        if (base.startsWith("msg_")) {
            base = base.substring(4);
        }
        String shardHex;
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] hash = digest.digest(base.getBytes("UTF-8"));
            StringBuilder hex = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                hex.append(String.format(Locale.US, "%02x", b));
            }
            shardHex = hex.toString();
        } catch (Exception e) {
            return null;
        }
        if (shardHex.length() < 4) {
            return null;
        }
        File[] profiles = microMsgRoot.listFiles();
        if (profiles == null) {
            return null;
        }
        String fileNameBase = "msg_" + base;
        for (File profile : profiles) {
            if (profile == null || !profile.isDirectory()) {
                continue;
            }
            File shardDir = new File(new File(new File(profile, "voice2"), shardHex.substring(0, 2)), shardHex.substring(2, 4));
            if (!shardDir.isDirectory()) {
                continue;
            }
            File amr = new File(shardDir, fileNameBase + ".amr");
            if (amr.isFile()) {
                return amr;
            }
            File silk = new File(shardDir, fileNameBase + ".silk");
            if (silk.isFile()) {
                return silk;
            }
        }
        return null;
    }

    private static void logVoiceMediaMissDiagnostic(File microMsgRoot, List<String> names) {
        try {
            File[] profiles = microMsgRoot.listFiles();
            if (profiles == null) {
                log("voice media miss: cannot list " + microMsgRoot.getAbsolutePath() + " (listFiles null) names=" + names);
                return;
            }
            StringBuilder sb = new StringBuilder("voice media miss names=").append(names).append(" profiles=[");
            for (File profile : profiles) {
                if (!profile.isDirectory()) {
                    continue;
                }
                File voice2 = new File(profile, "voice2");
                sb.append(profile.getName()).append("(voice2Exists=").append(voice2.isDirectory());
                if (voice2.isDirectory()) {
                    File[] shards = voice2.listFiles();
                    sb.append(",shardCount=").append(shards == null ? -1 : shards.length);
                    if (shards != null && shards.length > 0) {
                        File firstShard = shards[0];
                        sb.append(",firstShard=").append(firstShard.getName());
                        File[] inner = firstShard.listFiles();
                        sb.append(",firstShardChildren=").append(inner == null ? -1 : inner.length);
                    }
                }
                sb.append(") ");
            }
            sb.append("]");
            log(sb.toString());
        } catch (Throwable t) {
            log("voice media miss diagnostic crashed: " + shortError(rootCause(t)));
        }
    }

    private static File findMediaInProfileRoots(File profileRoot, String[] roots, List<String> names) {
        File[] profiles = profileRoot == null ? null : profileRoot.listFiles();
        if (profiles == null) {
            return null;
        }
        for (File profile : profiles) {
            if (profile == null || !profile.isDirectory()) {
                continue;
            }
            for (String rootName : roots) {
                File found = findNamedFile(new File(profile, rootName), names, 6, new int[]{0});
                if (found != null) {
                    return found;
                }
            }
        }
        return null;
    }

    private static File directMediaFile(String mediaHint) {
        if (isBlank(mediaHint)) {
            return null;
        }
        String[] candidates = new String[]{mediaHint.trim(), normalizeMediaHint(mediaHint)};
        Context context = APP_CONTEXT;
        File appRoot = null;
        if (context != null && context.getFilesDir() != null) {
            appRoot = context.getFilesDir().getParentFile();
        }
        for (String candidate : candidates) {
            if (isBlank(candidate)) {
                continue;
            }
            File direct = new File(candidate);
            if (direct.isFile()) {
                return direct;
            }
            if (appRoot != null) {
                File relative = new File(appRoot, candidate);
                if (relative.isFile()) {
                    return relative;
                }
            }
        }
        return null;
    }

    private static List<String> mediaCandidateNames(int type, String mediaHint) {
        if (type == 3) {
            return InboundImagePolicy.originalCandidateNames(mediaHint);
        }
        List<String> names = new ArrayList<>();
        String normalized = normalizeMediaHint(mediaHint);
        addMediaCandidate(names, normalized);
        int slash = Math.max(normalized.lastIndexOf('/'), normalized.lastIndexOf(File.separatorChar));
        if (slash >= 0 && slash + 1 < normalized.length()) {
            addMediaCandidate(names, normalized.substring(slash + 1));
        }
        List<String> snapshot = new ArrayList<>(names);
        if (type == 43 || type == 62) {
            List<String> videoNames = new ArrayList<>();
            for (String candidate : snapshot) {
                if (isBlank(candidate)) {
                    continue;
                }
                if (candidate.startsWith("th_") && candidate.length() > 3) {
                    String withoutThumb = candidate.substring(3);
                    addVideoCandidateVariants(videoNames, withoutThumb);
                }
                addVideoCandidateVariants(videoNames, candidate);
            }
            for (String candidate : snapshot) {
                addMediaCandidate(videoNames, candidate);
            }
            return videoNames;
        }
        for (String candidate : snapshot) {
            if (type == 34) {
                String base = stripKnownMediaExtension(candidate);
                if (!base.equals(candidate)) {
                    addMediaCandidate(names, base);
                }
                addMediaCandidate(names, base + ".amr");
                addMediaCandidate(names, base + ".silk");
                // The voiceinfo/voiceinfo2 FileName hint (e.g. "amr_<hash>") is not the actual
                // on-disk name once WeChat downloads the audio: it lands under voice2/<shard>/
                // as "msg_" + that hint + extension (confirmed against a real downloaded file:
                // voice2/96/99/msg_amr_<hash>.amr). Without this, a freshly-downloaded voice
                // file could never be found even though requestVoiceDownloadAsync succeeded.
                if (!base.startsWith("msg_")) {
                    addMediaCandidate(names, "msg_" + base);
                    addMediaCandidate(names, "msg_" + base + ".amr");
                    addMediaCandidate(names, "msg_" + base + ".silk");
                }
            }
        }
        return names;
    }

    private static void addVideoCandidateVariants(List<String> names, String candidate) {
        if (isBlank(candidate)) {
            return;
        }
        String value = candidate.trim();
        addMediaCandidate(names, value);
        String base = stripKnownMediaExtension(value);
        if (!base.equals(value)) {
            addMediaCandidate(names, base);
        }
        addMediaCandidate(names, base + ".mp4");
        if (!base.startsWith("video_")) {
            addMediaCandidate(names, "video_" + base);
            addMediaCandidate(names, "video_" + base + ".mp4");
        }
    }

    private static void addEmojiCandidateVariants(List<String> names, String emojiMd5) {
        if (isBlank(emojiMd5)) {
            return;
        }
        String md5 = emojiMd5.trim().toLowerCase(Locale.US);
        addMediaCandidate(names, md5);
        addMediaCandidate(names, md5 + ".gif");
        addMediaCandidate(names, md5 + ".png");
        addMediaCandidate(names, md5 + ".webp");
        addMediaCandidate(names, md5 + ".jpg");
        addMediaCandidate(names, "emoji_" + md5);
        addMediaCandidate(names, "emoji_" + md5 + ".gif");
        addMediaCandidate(names, "emoji_" + md5 + ".png");
        addMediaCandidate(names, "emoji_" + md5 + ".webp");
        addMediaCandidate(names, "thumb_" + md5);
        addMediaCandidate(names, "thumb_" + md5 + ".gif");
        addMediaCandidate(names, "thumb_" + md5 + ".png");
        addMediaCandidate(names, "thumb_" + md5 + ".webp");
    }

    private static String stripKnownMediaExtension(String value) {
        if (isBlank(value)) {
            return "";
        }
        String lower = value.toLowerCase(Locale.US);
        String[] extensions = new String[]{".mp4", ".jpg", ".jpeg", ".png", ".webp", ".amr", ".silk"};
        for (String extension : extensions) {
            if (lower.endsWith(extension) && value.length() > extension.length()) {
                return value.substring(0, value.length() - extension.length());
            }
        }
        return value;
    }

    private static String normalizeMediaHint(String mediaHint) {
        if (isBlank(mediaHint)) {
            return "";
        }
        String value = mediaHint.trim();
        int query = value.indexOf('?');
        if (query >= 0) {
            value = value.substring(0, query);
        }
        int scheme = value.indexOf("://");
        if (scheme >= 0 && scheme + 3 < value.length()) {
            value = value.substring(scheme + 3);
        }
        return value.trim();
    }

    private static void addMediaCandidate(List<String> names, String name) {
        if (isBlank(name)) {
            return;
        }
        String value = name.trim();
        for (String existing : names) {
            if (existing.equals(value)) {
                return;
            }
        }
        names.add(value);
    }

    private static String[] mediaSearchRoots(int type) {
        switch (type) {
            case 3:
                return new String[]{"image2"};
            case 34:
                return new String[]{"voice2"};
            case 43:
            case 62:
                return new String[]{"video", "c2c_temp/origin/video", "c2c_temp"};
            case 47:
                return emojiSearchRoots();
            case 49:
                return new String[]{"attachment", "openapi", "image2"};
            default:
                return new String[]{"image2", "voice2", "video", "attachment"};
        }
    }

    private static String[] videoCacheSearchRoots() {
        return new String[]{"finder/video", "video"};
    }

    private static String[] emojiSearchRoots() {
        return new String[]{"emoji", "emoji/egg", "emoji/custom", "emoji/panel", "emoticon-user-files", "emoticon-user-core"};
    }

    private static File findEmojiMediaOutsideProfile(File appRoot, List<String> names) {
        if (appRoot == null || names == null || names.isEmpty()) {
            return null;
        }
        File found = findNamedFile(new File(appRoot, "files/public/emoji"), names, 6, new int[]{0});
        if (found != null) {
            return found;
        }
        return findNamedFile(new File(appRoot, "cache"), names, 4, new int[]{0});
    }

    private static File findNamedFile(File root, List<String> names, int depth, int[] visited) {
        if (root == null || !root.isDirectory() || depth < 0 || visited[0] > 6000) {
            return null;
        }
        File[] files = root.listFiles();
        if (files == null) {
            return null;
        }
        for (File file : files) {
            if (file == null) {
                continue;
            }
            visited[0]++;
            if (file.isFile() && mediaNameMatches(file.getName(), names)) {
                return file;
            }
        }
        for (File file : files) {
            if (file != null && file.isDirectory()) {
                File found = findNamedFile(file, names, depth - 1, visited);
                if (found != null) {
                    return found;
                }
            }
        }
        return null;
    }

    private static boolean mediaNameMatches(String fileName, List<String> names) {
        if (isBlank(fileName)) {
            return false;
        }
        for (String name : names) {
            if (fileName.equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static byte[] readFileBytes(File file, long limit) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        long total = 0L;
        try (FileInputStream input = new FileInputStream(file)) {
            int read;
            while ((read = input.read(buffer)) != -1) {
                total += read;
                if (total > limit) {
                    throw new IOException("media file exceeds limit");
                }
                out.write(buffer, 0, read);
            }
        }
        return out.toByteArray();
    }

    private static String detectMediaMime(int type, File file, byte[] bytes) {
        String name = file == null ? "" : file.getName().toLowerCase(Locale.US);
        if (name.endsWith(".jpg") || name.endsWith(".jpeg") || startsWith(bytes, new byte[]{(byte) 0xff, (byte) 0xd8})) {
            return "image/jpeg";
        }
        if (name.endsWith(".png") || startsWith(bytes, new byte[]{(byte) 0x89, 0x50, 0x4e, 0x47})) {
            return "image/png";
        }
        if (name.endsWith(".gif") || startsWith(bytes, new byte[]{0x47, 0x49, 0x46})) {
            return "image/gif";
        }
        if (name.endsWith(".webp") || containsAsciiAt(bytes, "WEBP", 8)) {
            return "image/webp";
        }
        if (name.endsWith(".mp4") || containsAsciiAt(bytes, "ftyp", 4)) {
            return "video/mp4";
        }
        if (name.endsWith(".amr") || startsWith(bytes, "#!AMR".getBytes(StandardCharsets.US_ASCII))) {
            return "audio/amr";
        }
        if (name.endsWith(".silk") || startsWith(bytes, "#!SILK".getBytes(StandardCharsets.US_ASCII))) {
            return "audio/silk";
        }
        if (type == 34) {
            return "audio/amr";
        }
        if (type == 3) {
            return "image/jpeg";
        }
        if (type == 43 || type == 62) {
            return "video/mp4";
        }
        return "application/octet-stream";
    }

    private static boolean startsWith(byte[] bytes, byte[] prefix) {
        if (bytes == null || prefix == null || bytes.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (bytes[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    private static boolean containsAsciiAt(byte[] bytes, String text, int offset) {
        if (bytes == null || text == null || offset < 0 || bytes.length < offset + text.length()) {
            return false;
        }
        byte[] expected = text.getBytes(StandardCharsets.US_ASCII);
        for (int i = 0; i < expected.length; i++) {
            if (bytes[offset + i] != expected[i]) {
                return false;
            }
        }
        return true;
    }

    private static String mediaUploadName(File file, String mime, String id) {
        String name = file == null ? "" : file.getName();
        if (isBlank(name)) {
            name = isBlank(id) ? "media" : "media-" + id;
        }
        if (name.indexOf('.') >= 0) {
            return name;
        }
        return name + extensionForMime(mime);
    }

    private static String extensionForMime(String mime) {
        if ("image/png".equals(mime)) {
            return ".png";
        }
        if ("image/gif".equals(mime)) {
            return ".gif";
        }
        if ("image/webp".equals(mime)) {
            return ".webp";
        }
        if ("audio/amr".equals(mime)) {
            return ".amr";
        }
        if ("audio/silk".equals(mime)) {
            return ".silk";
        }
        if ("video/mp4".equals(mime)) {
            return ".mp4";
        }
        if ("image/jpeg".equals(mime)) {
            return ".jpg";
        }
        return ".bin";
    }

    private static String displayMessageText(String talker, String content, int type) {
        if (type == 1) {
            return normalizeMessageText(talker, content);
        }
        String label = messageTypeLabel(type);
        String detail = nonTextMessageDetail(type, normalizeMessageContent(talker, content, type));
        if (isBlank(detail)) {
            return label;
        }
        return label + " " + detail;
    }

    private static String nonTextMessageDetail(int type, String content) {
        String detail = firstNonBlank(
                extractXmlField(content, "title"),
                extractXmlField(content, "des"),
                extractXmlField(content, "filename"),
                extractXmlField(content, "appname"),
                extractXmlField(content, "displayname"));
        if (!isBlank(detail)) {
            return compactDisplayText(detail, 80);
        }
        if (!looksLikeXmlPayload(content)) {
            return compactDisplayText(content, 80);
        }
        if (type == 10000) {
            return "";
        }
        return "";
    }

    private static String messageTypeLabel(int type) {
        switch (type) {
            case 1:
                return "[文本]";
            case 3:
                return "[图片]";
            case 34:
                return "[语音]";
            case 37:
                return "[好友请求]";
            case 43:
                return "[视频]";
            case 47:
                return "[表情]";
            case 48:
                return "[位置]";
            case 49:
                return "[链接/文件]";
            case MESSAGE_TYPE_TRANSFER:
                return "[转账]";
            case MESSAGE_TYPE_RED_PACKET:
                return "[红包]";
            case MESSAGE_TYPE_FILE_TRANSFER:
                return "[文件]";
            case 50:
                return "[通话]";
            case 62:
                return "[小视频]";
            case 10000:
                return "[系统消息]";
            default:
                return "[消息 " + type + "]";
        }
    }

    private static boolean looksLikeXmlPayload(String value) {
        if (isBlank(value)) {
            return false;
        }
        String text = value.trim().toLowerCase(Locale.US);
        return text.startsWith("<msg")
                || text.startsWith("<?xml")
                || text.startsWith("<sysmsg")
                || text.startsWith("<appmsg")
                || text.startsWith("<emoji")
                || text.startsWith("<template>");
    }

    private static String rawXmlPayload(String content, int type) {
        if (type != 49 && type != 48 && type != 47 && type != 10000
                && type != MESSAGE_TYPE_TRANSFER && type != MESSAGE_TYPE_RED_PACKET) {
            return "";
        }
        if (!looksLikeXmlPayload(content)) {
            return "";
        }
        return content.trim();
    }

    private static String extractXmlField(String xml, String field) {
        return cleanDisplayText(extractXmlFieldRaw(xml, field), 120);
    }

    private static String xmlElementNames(String xml) {
        if (isBlank(xml)) {
            return "<empty>";
        }
        Set<String> names = new HashSet<>();
        int from = 0;
        while (from < xml.length() && names.size() < 48) {
            int start = xml.indexOf('<', from);
            if (start < 0 || start + 1 >= xml.length()) {
                break;
            }
            int cursor = start + 1;
            char marker = xml.charAt(cursor);
            if (marker == '/' || marker == '?' || marker == '!') {
                cursor++;
            }
            int nameStart = cursor;
            while (cursor < xml.length()) {
                char value = xml.charAt(cursor);
                if (!(Character.isLetterOrDigit(value) || value == '_' || value == ':' || value == '-')) {
                    break;
                }
                cursor++;
            }
            if (cursor > nameStart) {
                names.add(xml.substring(nameStart, cursor).toLowerCase(Locale.US));
            }
            from = Math.max(cursor, start + 1);
        }
        List<String> sorted = new ArrayList<>(names);
        Collections.sort(sorted);
        StringBuilder result = new StringBuilder();
        for (String name : sorted) {
            if (result.length() > 0) {
                result.append(',');
            }
            result.append(name);
        }
        return result.length() == 0 ? "<none>" : result.toString();
    }

    private static String extractXmlAttribute(String xml, String attribute) {
        if (isBlank(xml) || isBlank(attribute)) {
            return "";
        }
        String text = xml.trim();
        String lower = text.toLowerCase(Locale.US);
        String name = attribute.trim().toLowerCase(Locale.US);
        int searchFrom = 0;
        while (searchFrom < lower.length()) {
            int start = lower.indexOf(name, searchFrom);
            if (start < 0) {
                return "";
            }
            int before = start - 1;
            int afterName = start + name.length();
            boolean boundaryBefore = before < 0 || !isXmlNameChar(lower.charAt(before));
            boolean boundaryAfter = afterName < lower.length() && lower.charAt(afterName) == '=';
            if (!boundaryBefore || !boundaryAfter) {
                searchFrom = afterName;
                continue;
            }
            int valueStart = afterName + 1;
            while (valueStart < text.length() && Character.isWhitespace(text.charAt(valueStart))) {
                valueStart++;
            }
            if (valueStart >= text.length()) {
                return "";
            }
            char quote = text.charAt(valueStart);
            int valueEnd;
            if (quote == '"' || quote == '\'') {
                valueStart++;
                valueEnd = text.indexOf(quote, valueStart);
                if (valueEnd < 0) {
                    return "";
                }
            } else {
                valueEnd = valueStart;
                while (valueEnd < text.length()) {
                    char ch = text.charAt(valueEnd);
                    if (Character.isWhitespace(ch) || ch == '/' || ch == '>') {
                        break;
                    }
                    valueEnd++;
                }
            }
            return stripCData(text.substring(valueStart, valueEnd)).trim();
        }
        return "";
    }

    private static boolean isXmlNameChar(char ch) {
        return (ch >= 'a' && ch <= 'z')
                || (ch >= '0' && ch <= '9')
                || ch == '_'
                || ch == '-'
                || ch == ':';
    }

    private static String emojiMd5FromContent(String talker, String content) {
        String normalized = normalizeMessageContent(talker, content, 47);
        return firstNonBlank(
                normalizeEmojiMd5(extractXmlAttribute(normalized, "md5")),
                normalizeEmojiMd5(extractXmlAttribute(normalized, "androidmd5")),
                normalizeEmojiMd5(extractXmlAttribute(normalized, "externmd5")),
                normalizeEmojiMd5(extractXmlAttribute(content, "md5")),
                normalizeEmojiMd5(extractXmlAttribute(content, "androidmd5")),
                normalizeEmojiMd5(extractXmlAttribute(content, "externmd5")),
                emojiMd5FromColonPayload(normalizeMessageText(talker, content)));
    }

    private static String normalizeEmojiMd5(String value) {
        if (isBlank(value)) {
            return "";
        }
        return value.trim().toLowerCase(Locale.US);
    }

    private static String emojiMd5FromColonPayload(String content) {
        if (isBlank(content)) {
            return "";
        }
        String text = content.trim();
        String prefix = text;
        int xmlStart = embeddedXmlStart(text);
        if (xmlStart > 0) {
            prefix = text.substring(0, xmlStart);
        }
        String[] parts = prefix.split(":");
        for (String part : parts) {
            String candidate = normalizeEmojiMd5(part);
            if (looksLikeMd5Hex(candidate)) {
                return candidate;
            }
        }
        return "";
    }

    private static boolean looksLikeMd5Hex(String value) {
        if (isBlank(value) || value.length() != 32) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            boolean digit = ch >= '0' && ch <= '9';
            boolean lower = ch >= 'a' && ch <= 'f';
            boolean upper = ch >= 'A' && ch <= 'F';
            if (!digit && !lower && !upper) {
                return false;
            }
        }
        return true;
    }

    private static String extractXmlFieldRaw(String xml, String field) {
        if (isBlank(xml) || isBlank(field)) {
            return "";
        }
        String text = xml.trim();
        String lower = text.toLowerCase(Locale.US);
        String openTag = "<" + field.toLowerCase(Locale.US) + ">";
        int start = lower.indexOf(openTag);
        if (start >= 0) {
            start += openTag.length();
        } else {
            String prefix = "<" + field.toLowerCase(Locale.US);
            start = lower.indexOf(prefix);
            if (start < 0) {
                return "";
            }
            start = lower.indexOf(">", start);
            if (start < 0) {
                return "";
            }
            start++;
        }
        String closeTag = "</" + field.toLowerCase(Locale.US) + ">";
        int end = lower.indexOf(closeTag, start);
        if (end <= start) {
            return "";
        }
        return text.substring(start, end).trim();
    }

    private static String cleanDisplayText(String text, int maxLen) {
        if (isBlank(text)) {
            return "";
        }
        String value = text.trim()
                .replace("\r", " ")
                .replace("\n", " ")
                .replace("\t", " ");
        if (value.startsWith("<![CDATA[") && value.endsWith("]]>") && value.length() >= 12) {
            value = value.substring(9, value.length() - 3).trim();
        }
        value = value.replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&amp;", "&")
                .replace("&quot;", "\"")
                .replace("&#39;", "'");
        while (value.contains("  ")) {
            value = value.replace("  ", " ");
        }
        if (maxLen > 0 && value.length() > maxLen) {
            value = value.substring(0, maxLen).trim();
        }
        return value;
    }

    private static String compactDisplayText(String text, int maxLen) {
        return cleanDisplayText(text, maxLen);
    }

    private static String firstNonBlank(String... values) {
        if (values == null) {
            return "";
        }
        for (String value : values) {
            if (!isBlank(value)) {
                return value;
            }
        }
        return "";
    }

    private static long firstPositiveLong(long... values) {
        if (values == null) {
            return 0L;
        }
        for (long value : values) {
            if (value > 0L) {
                return value;
            }
        }
        return 0L;
    }

    private static int firstPositiveInt(int... values) {
        if (values == null) {
            return 0;
        }
        for (int value : values) {
            if (value > 0) {
                return value;
            }
        }
        return 0;
    }

    private static double firstFiniteDouble(double... values) {
        if (values == null) {
            return Double.NaN;
        }
        for (double value : values) {
            if (isFinite(value)) {
                return value;
            }
        }
        return Double.NaN;
    }

    private static boolean isFinite(double value) {
        return !Double.isNaN(value) && !Double.isInfinite(value);
    }

    private static boolean containsInt(int[] values, int expected) {
        if (values == null) {
            return false;
        }
        for (int value : values) {
            if (value == expected) {
                return true;
            }
        }
        return false;
    }

    private static boolean isChatroomTalker(String talker) {
        return !isBlank(talker) && talker.toLowerCase(Locale.US).endsWith("@chatroom");
    }

    private static String normalizeMessageText(String talker, String content) {
        if (isBlank(content)) {
            return "";
        }
        String text = content.trim();
        if (!isChatroomTalker(talker)) {
            return text;
        }
        int newline = text.indexOf(":\n");
        if (newline <= 0) {
            return text;
        }
        String prefix = text.substring(0, newline).trim();
        String body = text.substring(newline + 2).trim();
        if (looksLikeWxid(prefix) && !isBlank(body)) {
            return body;
        }
        return text;
    }

    private static String normalizeMessageContent(String talker, String content, int type) {
        String text = normalizeMessageText(talker, content);
        if (type != 47) {
            return text;
        }
        String embedded = embeddedXmlPayload(text);
        return isBlank(embedded) ? text : embedded;
    }

    private static String embeddedXmlPayload(String value) {
        int start = embeddedXmlStart(value);
        if (start < 0) {
            return "";
        }
        return value.substring(start).trim();
    }

    private static int embeddedXmlStart(String value) {
        if (isBlank(value)) {
            return -1;
        }
        String lower = value.toLowerCase(Locale.US);
        int best = -1;
        String[] markers = new String[]{"<msg", "<emoji", "<?xml", "<sysmsg", "<appmsg"};
        for (String marker : markers) {
            int index = lower.indexOf(marker);
            if (index >= 0 && (best < 0 || index < best)) {
                best = index;
            }
        }
        return best;
    }

    private static String extractChatroomSender(String content) {
        if (isBlank(content)) {
            return "";
        }
        String text = content.trim();
        int newline = text.indexOf(":\n");
        if (newline <= 0) {
            return "";
        }
        String prefix = text.substring(0, newline).trim();
        return looksLikeWxid(prefix) ? prefix : "";
    }

    private static boolean looksLikeWxid(String value) {
        if (isBlank(value)) {
            return false;
        }
        String normalized = value.trim();
        if (normalized.length() < 3 || normalized.indexOf(' ') >= 0 || normalized.indexOf('\n') >= 0 || normalized.indexOf('\r') >= 0) {
            return false;
        }
        return normalized.startsWith("wxid_")
                || normalized.startsWith("gh_")
                || normalized.contains("@chatroom")
                || normalized.contains("_");
    }

    private static JSONArray readContacts(Object db, BridgeConfig config) throws Exception {
        int limit = config.contactSyncLimit <= 0 ? 1000 : Math.min(config.contactSyncLimit, 10000);
        Object cursor = rawQuery(db, ""
                + "SELECT username,nickname,conRemark,alias,type,verifyFlag "
                + "FROM rcontact "
                + "WHERE username IS NOT NULL AND username <> '' "
                + "LIMIT ?", new String[]{String.valueOf(limit)});
        JSONArray out = new JSONArray();
        Set<String> seen = new HashSet<>();
        if (cursor == null) {
            return out;
        }
        try {
            Method moveToNext = findNoArgMethod(cursor.getClass(), "moveToNext");
            while (Boolean.TRUE.equals(moveToNext.invoke(cursor))) {
                String wxid = stringColumn(cursor, 0);
                int type = intColumn(cursor, 4);
                boolean chatroom = wxid.toLowerCase(Locale.US).endsWith("@chatroom");
                if (!shouldIncludeContact(wxid, type, chatroom, config)) {
                    continue;
                }
                String normalized = wxid.trim().toLowerCase(Locale.US);
                if (!seen.add(normalized)) {
                    continue;
                }
                JSONObject contact = new JSONObject();
                contact.put("wxid", wxid);
                contact.put("nickname", contactNickname(wxid, stringColumn(cursor, 1)));
                contact.put("remark", stringColumn(cursor, 2));
                contact.put("alias", stringColumn(cursor, 3));
                contact.put("type", type);
                contact.put("verify_flag", intColumn(cursor, 5));
                contact.put("chatroom", chatroom);
                contact.put("deleted", false);
                out.put(contact);
            }
        } finally {
            closeQuietly(cursor);
        }
        if (config.includeChatrooms && out.length() < limit) {
            int added = appendChatroomContacts(db, out, seen, limit - out.length());
            if (added == 0 && out.length() < limit) {
                appendActiveDatabaseChatrooms(out, seen, limit - out.length());
            }
        }
        return out;
    }

    private static int appendChatroomContacts(Object db, JSONArray out, Set<String> seen, int limit) {
        if (limit <= 0) {
            return 0;
        }
        Object cursor = null;
        try {
            cursor = rawQuery(db, ""
                    + "SELECT chatroomname,displayname "
                    + "FROM chatroom "
                    + "WHERE chatroomname IS NOT NULL AND chatroomname <> '' "
                    + "LIMIT ?", new String[]{String.valueOf(limit)});
        } catch (Throwable first) {
            try {
                cursor = rawQuery(db, ""
                        + "SELECT chatroomname,'' "
                        + "FROM chatroom "
                        + "WHERE chatroomname IS NOT NULL AND chatroomname <> '' "
                        + "LIMIT ?", new String[]{String.valueOf(limit)});
            } catch (Throwable second) {
                log("chatroom contact scan skipped: " + shortError(second));
                return 0;
            }
        }
        if (cursor == null) {
            return 0;
        }
        int added = 0;
        try {
            Method moveToNext = findNoArgMethod(cursor.getClass(), "moveToNext");
            while (Boolean.TRUE.equals(moveToNext.invoke(cursor))) {
                String wxid = stringColumn(cursor, 0);
                if (isBlank(wxid) || !wxid.toLowerCase(Locale.US).endsWith("@chatroom")) {
                    continue;
                }
                String normalized = wxid.trim().toLowerCase(Locale.US);
                if (!seen.add(normalized)) {
                    continue;
                }
                String nickname = stringColumn(cursor, 1);
                JSONObject contact = new JSONObject();
                contact.put("wxid", wxid);
                contact.put("nickname", isBlank(nickname) ? wxid : nickname);
                contact.put("remark", "");
                contact.put("alias", "");
                contact.put("type", 0);
                contact.put("verify_flag", 0);
                contact.put("chatroom", true);
                contact.put("deleted", false);
                out.put(contact);
                added++;
            }
        } catch (Throwable t) {
            log("chatroom contact scan failed: " + shortError(t));
        } finally {
            closeQuietly(cursor);
        }
        if (added > 0) {
            log("chatroom contact scan added=" + added);
        }
        return added;
    }

    private static void appendActiveDatabaseChatrooms(JSONArray out, Set<String> seen, int limit) {
        if (limit <= 0) {
            return;
        }
        try {
            ClassLoader classLoader = WECHAT_CLASS_LOADER;
            if (classLoader == null) {
                classLoader = runtimeClassLoader(HookEntry.class.getClassLoader());
            } else {
                classLoader = runtimeClassLoader(classLoader);
            }
            Class<?> dbClass = findClass(classLoader, "com.tencent.wcdb.database.SQLiteDatabase");
            Field activeField = findField(dbClass, "sActiveDatabases");
            Object active = activeField.get(null);
            if (!(active instanceof Map)) {
                return;
            }
            Object[] databases;
            synchronized (active) {
                databases = ((Map<?, ?>) active).keySet().toArray();
            }
            int total = 0;
            for (Object candidate : databases) {
                if (candidate == null || out.length() >= limit) {
                    continue;
                }
                int added = appendChatroomContacts(candidate, out, seen, limit - out.length());
                if (added > 0) {
                    total += added;
                    log("chatroom active database path=" + databasePath(candidate) + " added=" + added);
                }
            }
            if (total == 0) {
                log("chatroom active database scan found no rows dbCount=" + databases.length);
            }
        } catch (Throwable t) {
            log("chatroom active database scan failed: " + shortError(t));
        }
    }

    private static Object findContactDatabase(BridgeConfig config) {
        try {
            ClassLoader classLoader = WECHAT_CLASS_LOADER;
            if (classLoader == null) {
                classLoader = runtimeClassLoader(HookEntry.class.getClassLoader());
            } else {
                classLoader = runtimeClassLoader(classLoader);
            }
            Class<?> dbClass = findClass(classLoader, "com.tencent.wcdb.database.SQLiteDatabase");
            Field activeField = findField(dbClass, "sActiveDatabases");
            Object active = activeField.get(null);
            if (!(active instanceof Map)) {
                return null;
            }
            Object[] databases;
            synchronized (active) {
                databases = ((Map<?, ?>) active).keySet().toArray();
            }
            boolean verboseScanLog = shouldLogContactScan();
            if (verboseScanLog) {
                log("contact database scan active db count=" + databases.length);
            }
            for (Object db : databases) {
                if (db == null) {
                    continue;
                }
                try {
                    JSONArray contacts = readContacts(db, config);
                    if (contacts.length() > 0) {
                        if (!hasMessageTable(db)) {
                            if (LAST_DATABASE == db) {
                                LAST_DATABASE = null;
                            }
                            if (verboseScanLog) {
                                log("contact database candidate has no message table path=" + databasePath(db));
                            }
                            continue;
                        }
                        LAST_DATABASE = db;
                        log("captured WeChat database from active set path=" + databasePath(db)
                                + " contacts=" + contacts.length());
                        return db;
                    }
                    if (verboseScanLog) {
                        log("contact database candidate empty path=" + databasePath(db));
                    }
                } catch (Throwable ignored) {
                    if (verboseScanLog) {
                        log("contact database candidate failed path=" + databasePath(db)
                                + " error=" + shortError(ignored));
                    }
                }
            }
        } catch (Throwable t) {
            log("contact database scan failed: " + shortError(t));
        }
        return null;
    }

    private static boolean shouldLogContactScan() {
        long now = System.currentTimeMillis();
        if (now - LAST_CONTACT_SCAN_LOG_AT < 60000L) {
            return false;
        }
        LAST_CONTACT_SCAN_LOG_AT = now;
        return true;
    }

    private static Object findContactDatabaseOnMainThread(final BridgeConfig config) {
        if (Looper.myLooper() == Looper.getMainLooper()) {
            return findContactDatabase(config);
        }
        FutureTask<Object> task = new FutureTask<>(new Callable<Object>() {
            @Override
            public Object call() {
                return findContactDatabase(config);
            }
        });
        new Handler(Looper.getMainLooper()).post(task);
        try {
            return task.get();
        } catch (Throwable t) {
            log("contact database main-thread scan failed: " + shortError(t));
            return null;
        }
    }

    private static String databasePath(Object db) {
        try {
            Object value = findNoArgMethod(db.getClass(), "getPath").invoke(db);
            return value == null ? "<unknown>" : String.valueOf(value);
        } catch (Throwable ignored) {
            return "<unknown>";
        }
    }

    private static Object rawQuery(Object db, String sql, String[] args) throws Exception {
        try {
            Method rawQuery = findMethod(db.getClass(), "rawQuery", String.class, Object[].class);
            return rawQuery.invoke(db, sql, (Object) args);
        } catch (NoSuchMethodException ignored) {
            try {
                Method rawQuery = findMethod(db.getClass(), "rawQuery", String.class, Object[].class, findClass(db.getClass().getClassLoader(), "com.tencent.wcdb.support.CancellationSignal"));
                return rawQuery.invoke(db, sql, (Object) args, null);
            } catch (NoSuchMethodException ignoredObjectOverload) {
                try {
                    Method rawQuery = findMethod(db.getClass(), "rawQuery", String.class, String[].class);
                    return rawQuery.invoke(db, sql, (Object) args);
                } catch (NoSuchMethodException ignoredStringOverload) {
                    Method rawQuery = findMethod(db.getClass(), "rawQuery", String.class, String[].class, int.class);
                    return rawQuery.invoke(db, sql, (Object) args, 0);
                }
            }
        }
    }

    private static boolean shouldIncludeContact(String wxid, int type, boolean chatroom, BridgeConfig config) {
        if (isBlank(wxid)) {
            return false;
        }
        String normalized = wxid.trim().toLowerCase(Locale.US);
        if (!isBlank(config.selfWxid) && normalized.equals(config.selfWxid.trim().toLowerCase(Locale.US))) {
            return false;
        }
        if (isFileHelperContact(normalized)) {
            return true;
        }
        if (chatroom) {
            return config.includeChatrooms;
        }
        if ((type & 1) == 0) {
            return false;
        }
        if (normalized.startsWith("gh_")) {
            return false;
        }
        return !isSystemContact(normalized);
    }

    private static String contactNickname(String wxid, String nickname) {
        if (isFileHelperContact(wxid) && isBlank(nickname)) {
            return "文件传输助手";
        }
        return nickname;
    }

    private static boolean isFileHelperContact(String wxid) {
        return "filehelper".equals(wxid == null ? "" : wxid.trim().toLowerCase(Locale.US));
    }

    private static boolean isSystemContact(String wxid) {
        return "weixin".equals(wxid)
                || "medianote".equals(wxid)
                || "fmessage".equals(wxid)
                || "qmessage".equals(wxid)
                || "tmessage".equals(wxid)
                || "qqmail".equals(wxid)
                || "qqsync".equals(wxid)
                || "lbsapp".equals(wxid)
                || "shakeapp".equals(wxid)
                || "feedsapp".equals(wxid)
                || "masssendapp".equals(wxid)
                || "newsapp".equals(wxid)
                || "blogapp".equals(wxid)
                || "officialaccounts".equals(wxid)
                || "helper_entry".equals(wxid)
                || "voiceinputapp".equals(wxid)
                || "voicevoipapp".equals(wxid)
                || "weixinreminder".equals(wxid)
                || "notifymessage".equals(wxid);
    }

    private static String stringColumn(Object cursor, int index) {
        try {
            Object value = findMethod(cursor.getClass(), "getString", int.class).invoke(cursor, index);
            return value == null ? "" : String.valueOf(value);
        } catch (Throwable t) {
            return "";
        }
    }

    private static int intColumn(Object cursor, int index) {
        try {
            Object value = findMethod(cursor.getClass(), "getInt", int.class).invoke(cursor, index);
            if (value instanceof Number) {
                return ((Number) value).intValue();
            }
            return Integer.parseInt(String.valueOf(value));
        } catch (Throwable t) {
            return 0;
        }
    }

    private static long longColumn(Object cursor, int index) {
        try {
            Object value = findMethod(cursor.getClass(), "getLong", int.class).invoke(cursor, index);
            if (value instanceof Number) {
                return ((Number) value).longValue();
            }
            return Long.parseLong(String.valueOf(value));
        } catch (Throwable t) {
            return 0L;
        }
    }

    private static void closeQuietly(Object closeable) {
        if (closeable == null) {
            return;
        }
        try {
            findNoArgMethod(closeable.getClass(), "close").invoke(closeable);
        } catch (Throwable ignored) {
            // Ignore close failures from WeChat cursor implementations.
        }
    }

    private static void pollOutbox(BridgeConfig config, ClassLoader classLoader) throws Exception {
        String body = "{"
                + "\"api_key\":\"" + json(config.apiKey) + "\","
                + "\"device\":\"" + json(config.device) + "\","
                + "\"wxid\":\"" + json(config.selfWxid) + "\","
                + "\"limit\":" + config.pollLimit
                + "}";
        String response = postJson(config, "/module/outbox/poll", body);
        JSONObject root = new JSONObject(response);
        JSONArray items = root.optJSONArray("items");
        if (items == null || items.length() == 0) {
            return;
        }

        JSONArray ackItems = handleOutboxItems(items, classLoader, config);
        if (ackItems.length() == 0) {
            return;
        }
        JSONObject ackBody = new JSONObject();
        ackBody.put("api_key", config.apiKey);
        ackBody.put("device", config.device);
        ackBody.put("wxid", config.selfWxid);
        ackBody.put("items", ackItems);
        postJson(config, "/module/outbox/ack", ackBody.toString());
    }

    private static boolean runOutboxWebSocket(BridgeConfig config, ClassLoader classLoader) {
        if (!supportsPlainHttp(config)) {
            return false;
        }
        try {
            websocketLoop(config, classLoader);
        } catch (Throwable t) {
            logWebSocketFailure("outbox websocket unavailable: " + shortError(t));
            return false;
        }
        return false;
    }

    private static void websocketLoop(BridgeConfig config, ClassLoader classLoader) throws Exception {
        URL base = new URL(trimRight(config.baseUrl, "/"));
        int port = base.getPort() > 0 ? base.getPort() : 80;
        String host = base.getHost();
        String hostHeader = base.getPort() > 0 ? host + ":" + port : host;
        String path = trimRight(base.getPath(), "/") + "/module/outbox/ws"
                + "?api_key=" + urlEncode(config.apiKey)
                + "&device=" + urlEncode(config.device)
                + "&wxid=" + urlEncode(config.selfWxid)
                + "&limit=" + config.pollLimit;
        if (path.startsWith("//")) {
            path = path.substring(1);
        }
        byte[] nonce = new byte[16];
        new SecureRandom().nextBytes(nonce);
        String key = Base64.encodeToString(nonce, Base64.NO_WRAP);

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 5000);
            socket.setSoTimeout(30000);
            InputStream input = socket.getInputStream();
            OutputStream output = socket.getOutputStream();
            String request = "GET " + path + " HTTP/1.1\r\n"
                    + "Host: " + hostHeader + "\r\n"
                    + "Upgrade: websocket\r\n"
                    + "Connection: Upgrade\r\n"
                    + "Sec-WebSocket-Version: 13\r\n"
                    + "Sec-WebSocket-Key: " + key + "\r\n"
                    + "\r\n";
            output.write(request.getBytes(StandardCharsets.ISO_8859_1));
            output.flush();
            verifyWebSocketHandshake(input, key);
            log("outbox websocket connected host=" + hostHeader + " device=" + config.device);
            writeWebSocketText(output, "{\"type\":\"poll\"}");

            while (true) {
                if (configChanged(config)) {
                    log("outbox websocket config changed; reconnect");
                    return;
                }
                WebSocketFrame frame = readWebSocketFrame(input);
                if (frame.opcode == 0x8) {
                    return;
                }
                if (frame.opcode == 0x9) {
                    writeWebSocketFrame(output, 0xA, frame.payload);
                    continue;
                }
                if (frame.opcode == 0xA) {
                    continue;
                }
                if (frame.opcode != 0x1) {
                    continue;
                }
                JSONObject root = new JSONObject(new String(frame.payload, StandardCharsets.UTF_8));
                String type = root.optString("type", "");
                if ("outbox".equals(type)) {
                    JSONArray ackItems = handleOutboxItems(root.optJSONArray("items"), classLoader, config);
                    if (ackItems.length() > 0) {
                        JSONObject ackBody = new JSONObject();
                        ackBody.put("type", "ack");
                        JSONObject ack = new JSONObject();
                        ack.put("api_key", config.apiKey);
                        ack.put("device", config.device);
                        ack.put("wxid", config.selfWxid);
                        ack.put("items", ackItems);
                        ackBody.put("ack", ack);
                        writeWebSocketText(output, ackBody.toString());
                    }
                } else if ("ready".equals(type)) {
                    log("outbox websocket ready");
                } else if ("error".equals(type)) {
                    log("outbox websocket server error: " + root.optString("error", ""));
                }
            }
        }
    }

    private static boolean configChanged(BridgeConfig config) {
        BridgeConfig latest = BridgeConfig.load(bridgeContext());
        return !String.valueOf(config.signature).equals(String.valueOf(latest.signature));
    }

    private static JSONArray handleOutboxItems(JSONArray items, ClassLoader classLoader, BridgeConfig config) throws Exception {
        List<OutboxDispatchItem> dispatchItems = buildOutboxDispatchItems(items);
        if (dispatchItems.isEmpty()) {
            return new JSONArray();
        }
        int parallelism = normalizeOutboxParallelism(config, dispatchItems.size());
        if (parallelism <= 1 || dispatchItems.size() <= 1) {
            return toAckJSONArray(processOutboxLane(dispatchItems, classLoader, config));
        }

        LinkedHashMap<String, List<OutboxDispatchItem>> lanes = new LinkedHashMap<>();
        for (OutboxDispatchItem dispatchItem : dispatchItems) {
            List<OutboxDispatchItem> lane = lanes.get(dispatchItem.laneKey);
            if (lane == null) {
                lane = new ArrayList<>();
                lanes.put(dispatchItem.laneKey, lane);
            }
            lane.add(dispatchItem);
        }
        if (lanes.size() <= 1) {
            return toAckJSONArray(processOutboxLane(dispatchItems, classLoader, config));
        }

        int workerCount = Math.min(parallelism, lanes.size());
        ExecutorService executor = Executors.newFixedThreadPool(workerCount, new ThreadFactory() {
            private int nextIndex = 1;

            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable, "wechat-observatory-outbox-lane-" + nextIndex++);
                thread.setDaemon(true);
                return thread;
            }
        });
        try {
            List<FutureTask<List<OutboxAckEntry>>> tasks = new ArrayList<>();
            for (List<OutboxDispatchItem> laneItems : lanes.values()) {
                FutureTask<List<OutboxAckEntry>> task = new FutureTask<>(new Callable<List<OutboxAckEntry>>() {
                    @Override
                    public List<OutboxAckEntry> call() throws Exception {
                        return processOutboxLane(laneItems, classLoader, config);
                    }
                });
                tasks.add(task);
                executor.execute(task);
            }
            List<OutboxAckEntry> ackEntries = new ArrayList<>();
            for (FutureTask<List<OutboxAckEntry>> task : tasks) {
                ackEntries.addAll(task.get());
            }
            return toAckJSONArray(ackEntries);
        } finally {
            executor.shutdownNow();
        }
    }

    private static List<OutboxDispatchItem> buildOutboxDispatchItems(JSONArray items) throws Exception {
        List<OutboxDispatchItem> dispatchItems = new ArrayList<>();
        if (items == null) {
            return dispatchItems;
        }
        for (int i = 0; i < items.length(); i++) {
            JSONObject item = items.getJSONObject(i);
            dispatchItems.add(new OutboxDispatchItem(
                    i,
                    item,
                    item.optLong("id", 0L),
                    item.optString("wxid", ""),
                    outboxKind(item)));
        }
        return dispatchItems;
    }

    private static int normalizeOutboxParallelism(BridgeConfig config, int itemCount) {
        int parallelism = config == null ? 1 : config.outboxParallelism;
        if (parallelism <= 0) {
            parallelism = 1;
        }
        if (parallelism > 4) {
            parallelism = 4;
        }
        return Math.min(parallelism, Math.max(1, itemCount));
    }

    private static List<OutboxAckEntry> processOutboxLane(List<OutboxDispatchItem> laneItems, ClassLoader classLoader, BridgeConfig config) throws Exception {
        List<OutboxAckEntry> ackEntries = new ArrayList<>();
        for (OutboxDispatchItem dispatchItem : laneItems) {
            if (dispatchItem.id <= 0L) {
                continue;
            }
            SendResult result;
            try {
                result = sendOutboxItem(dispatchItem, classLoader, config);
            } catch (Throwable t) {
                result = SendResult.failed("outbox lane send failed: " + shortError(t));
            }
            ackEntries.add(new OutboxAckEntry(dispatchItem.index, outboxAck(dispatchItem.id, result)));
        }
        return ackEntries;
    }

    private static JSONArray toAckJSONArray(List<OutboxAckEntry> ackEntries) {
        Collections.sort(ackEntries, new Comparator<OutboxAckEntry>() {
            @Override
            public int compare(OutboxAckEntry left, OutboxAckEntry right) {
                return left.index - right.index;
            }
        });
        JSONArray ackItems = new JSONArray();
        for (OutboxAckEntry entry : ackEntries) {
            ackItems.put(entry.ack);
        }
        return ackItems;
    }

    private static SendResult sendOutboxItem(OutboxDispatchItem dispatchItem, ClassLoader classLoader, BridgeConfig config) {
        if (dispatchItem == null) {
            return SendResult.failed("outbox item is required");
        }
        if ("transfer_accept".equals(dispatchItem.kind)) {
            return sendTransferAcceptAction(config, classLoader, dispatchItem.item);
        }
        if ("transfer_decline".equals(dispatchItem.kind)) {
            return sendTransferDeclineAction(config, classLoader, dispatchItem.item);
        }
        if (isBlank(dispatchItem.wxid)) {
            return SendResult.failed("wxid is required");
        }
        JSONObject item = dispatchItem.item;
        String text = item.optString("text", "");
        if ("text".equals(dispatchItem.kind)) {
            return isBlank(text)
                    ? SendResult.failed("text is required")
                    : sendText(classLoader, dispatchItem.wxid, text);
        }
        if ("image".equals(dispatchItem.kind)) {
            return sendImageAction(config, classLoader, dispatchItem.wxid, item);
        }
        if ("video".equals(dispatchItem.kind)) {
            return sendVideoAction(config, classLoader, dispatchItem.wxid, item);
        }
        if ("voice".equals(dispatchItem.kind)) {
            return sendVoiceAction(config, classLoader, dispatchItem.wxid, item);
        }
        if ("file".equals(dispatchItem.kind)) {
            return sendFileAction(config, classLoader, dispatchItem.wxid, item);
        }
        if ("emoji".equals(dispatchItem.kind)) {
            return sendEmojiAction(config, classLoader, dispatchItem.wxid, item);
        }
        if ("location".equals(dispatchItem.kind)) {
            return sendLocationAction(config, classLoader, dispatchItem.wxid, text, item);
        }
        if ("quote".equals(dispatchItem.kind)) {
            return sendQuoteAction(config, classLoader, dispatchItem.wxid, text, item);
        }
        if ("link".equals(dispatchItem.kind)) {
            return sendLinkAction(config, classLoader, dispatchItem.wxid, text, item);
        }
        if ("revoke".equals(dispatchItem.kind)) {
            return sendRevokeAction(config, classLoader, dispatchItem.wxid, item);
        }
        if ("mini_program".equals(dispatchItem.kind)) {
            return sendMiniProgramAction(config, classLoader, dispatchItem.wxid, text, item);
        }
        if ("chat_history".equals(dispatchItem.kind)) {
            return sendChatHistoryAction(config, classLoader, dispatchItem.wxid, text, item);
        }
        return SendResult.failed("unsupported outbox kind: " + dispatchItem.kind);
    }

    private static SendResult sendTransferAcceptAction(BridgeConfig config, ClassLoader classLoader, JSONObject item) {
        try {
            JSONObject payload = item.optJSONObject("payload_json");
            long sourceChatRecordId = firstPositiveLong(
                    item.optLong("chat_record_id", 0L),
                    item.optLong("source_chat_record_id", 0L),
                    payload == null ? 0L : payload.optLong("chat_record_id", 0L),
                    payload == null ? 0L : payload.optLong("source_chat_record_id", 0L));
            if (sourceChatRecordId <= 0L) {
                return SendResult.failed("chat_record_id is required");
            }
            Object db = ensureMessageDatabase(config);
            if (db == null) {
                return SendResult.failed("WeChat message database is not available for transfer lookup");
            }
            ChatHistorySource source = loadChatHistorySource(db, sourceChatRecordId);
            if (source == null) {
                return SendResult.failed("source transfer message was not found in local WeChat message database");
            }
            if (source.isSend != 0) {
                return SendResult.failed("source_chat_record_id must be an inbound transfer");
            }
            String transferXML = normalizeMessageContent(source.talker, source.content, source.type);
            TransferMessageParser.Result transfer = TransferMessageParser.parse(transferXML);
            boolean transferType = source.type == MESSAGE_TYPE_TRANSFER
                    || (source.type == MESSAGE_TYPE_APPMSG && transfer.appMsgType == TransferMessageParser.APPMSG_TYPE_TRANSFER);
            if (!transferType || !transfer.isTransfer()) {
                return SendResult.failed("source_chat_record_id is not a transfer message");
            }
            if (!transfer.isPending()) {
                return SendResult.failed("transfer is not pending: " + transfer.state);
            }
            if (!transfer.hasConfirmFields()) {
                log("transfer confirm fields missing xmlLength=" + transferXML.length()
                        + " tags=" + xmlElementNames(transferXML));
                return SendResult.failed("transfer message is missing confirm identifiers or amount");
            }
            String groupUsername = isChatroomTalker(source.talker) ? source.talker : "";
            String sourceSender = isBlank(groupUsername) ? source.talker : extractChatroomSender(source.content);
            if (isBlank(sourceSender)) {
                return SendResult.failed("transfer sender cannot be resolved from the source message");
            }
            if (!isBlank(transfer.payerWxid) && !sourceSender.equals(transfer.payerWxid)) {
                return SendResult.failed("transfer payer does not match the source message sender");
            }
            String currentWxid = firstNonBlank(CURRENT_WXID, config == null ? "" : config.selfWxid);
            if (!isBlank(transfer.receiverWxid) && !isBlank(currentWxid) && !currentWxid.equals(transfer.receiverWxid)) {
                return SendResult.failed("transfer receiver does not match the current WeChat account");
            }
            return confirmTransfer(classLoader, sourceChatRecordId, sourceSender, groupUsername, transfer, "confirm");
        } catch (Throwable t) {
            log("transfer diag: accept failed: " + fullErrorChain(t));
            return SendResult.failed("transfer accept failed: " + shortError(t));
        }
    }

    private static SendResult sendTransferDeclineAction(BridgeConfig config, ClassLoader classLoader, JSONObject item) {
        try {
            JSONObject payload = item.optJSONObject("payload_json");
            long sourceChatRecordId = firstPositiveLong(
                    item.optLong("chat_record_id", 0L),
                    item.optLong("source_chat_record_id", 0L),
                    payload == null ? 0L : payload.optLong("chat_record_id", 0L),
                    payload == null ? 0L : payload.optLong("source_chat_record_id", 0L));
            if (sourceChatRecordId <= 0L) {
                return SendResult.failed("chat_record_id is required");
            }
            Object db = ensureMessageDatabase(config);
            if (db == null) {
                return SendResult.failed("WeChat message database is not available for transfer lookup");
            }
            ChatHistorySource source = loadChatHistorySource(db, sourceChatRecordId);
            if (source == null) {
                return SendResult.failed("source transfer message was not found in local WeChat message database");
            }
            if (source.isSend != 0) {
                return SendResult.failed("source_chat_record_id must be an inbound transfer");
            }
            String transferXML = normalizeMessageContent(source.talker, source.content, source.type);
            TransferMessageParser.Result transfer = TransferMessageParser.parse(transferXML);
            boolean transferType = source.type == MESSAGE_TYPE_TRANSFER
                    || (source.type == MESSAGE_TYPE_APPMSG && transfer.appMsgType == TransferMessageParser.APPMSG_TYPE_TRANSFER);
            if (!transferType || !transfer.isTransfer()) {
                return SendResult.failed("source_chat_record_id is not a transfer message");
            }
            if (!transfer.isPending()) {
                return SendResult.failed("transfer is not pending: " + transfer.state);
            }
            if (!transfer.hasConfirmFields()) {
                log("transfer confirm fields missing xmlLength=" + transferXML.length()
                        + " tags=" + xmlElementNames(transferXML));
                return SendResult.failed("transfer message is missing confirm identifiers or amount");
            }
            String groupUsername = isChatroomTalker(source.talker) ? source.talker : "";
            String sourceSender = isBlank(groupUsername) ? source.talker : extractChatroomSender(source.content);
            if (isBlank(sourceSender)) {
                return SendResult.failed("transfer sender cannot be resolved from the source message");
            }
            if (!isBlank(transfer.payerWxid) && !sourceSender.equals(transfer.payerWxid)) {
                return SendResult.failed("transfer payer does not match the source message sender");
            }
            String currentWxid = firstNonBlank(CURRENT_WXID, config == null ? "" : config.selfWxid);
            if (!isBlank(transfer.receiverWxid) && !isBlank(currentWxid) && !currentWxid.equals(transfer.receiverWxid)) {
                return SendResult.failed("transfer receiver does not match the current WeChat account");
            }
            return confirmTransfer(classLoader, sourceChatRecordId, sourceSender, groupUsername, transfer, "refuse");
        } catch (Throwable t) {
            log("transfer diag: decline failed: " + fullErrorChain(t));
            return SendResult.failed("transfer decline failed: " + shortError(t));
        }
    }

    private static SendResult confirmTransfer(
            ClassLoader classLoader,
            long sourceChatRecordId,
            String sourceSender,
            String groupUsername,
            TransferMessageParser.Result transfer,
            String operation) throws Exception {
        // z2/m1's Kernel-readiness state is per-ClassLoader; WeChat's Tinker hot-patch
        // loads a second copy of these classes under a different loader (see
        // runtimeClassLoader()'s isPatchedWeChatLoader preference). Resolving here,
        // before any class in this flow gets touched, keeps confirmTransfer/
        // queryTransferStatus/enqueueNetScene all on the same (patched) loader that
        // sendViaNetScene already uses successfully for text.
        classLoader = runtimeClassLoader(classLoader);
        ensureWeChatRegistriesForTransfer(classLoader);
        probeTransferClassChain(classLoader);
        int initialStatus = 0;
        try {
            initialStatus = queryTransferStatus(classLoader, transfer, groupUsername, 1, 10L);
        } catch (Throwable t) {
            log("transfer pre-confirm query unavailable: " + fullErrorChain(t));
        }
        if (initialStatus == 2001) {
            if ("refuse".equals(operation)) {
                return SendResult.failed("transfer was already received; it can no longer be declined");
            }
            log("transfer already accepted before confirm source_chat_record_id=" + sourceChatRecordId);
            return SendResult.sent(sourceChatRecordId);
        }
        if (isTerminalTransferStatus(initialStatus)) {
            return SendResult.failed("transfer is no longer receivable: status=" + initialStatus);
        }
        Class<?> confirmClass = findClass(classLoader, "com.tencent.mm.plugin.remittance.model.n0");
        ensureTransferCallbackHook(confirmClass, TRANSFER_CONFIRM_CALLBACK_HOOKED, TRANSFER_CONFIRM_WAITERS);
        Constructor<?> confirmConstructor = findConstructor(
                confirmClass,
                String.class,
                String.class,
                int.class,
                String.class,
                String.class,
                int.class,
                String.class,
                int.class,
                String.class,
                Map.class,
                long.class,
                String.class,
                String.class);
        Object confirmScene = newTransferScene(
                confirmConstructor,
                transfer.transactionId,
                transfer.transferId,
                (int) transfer.amountMinor,
                operation,
                sourceSender,
                transfer.invalidTime,
                groupUsername,
                1,
                "",
                null,
                0L,
                sourceSender,
                transfer.attach);
        NetworkResultWaiter confirmWaiter = new NetworkResultWaiter();
        TRANSFER_CONFIRM_WAITERS.put(confirmScene, confirmWaiter);
        try {
            setNetSceneProcessName(confirmScene, "RemittanceProcess");
            enqueueNetScene(classLoader, confirmScene);
            if (!confirmWaiter.await(20L)) {
                log("transfer confirm callback timed out; verifying with transfer query");
            } else if (confirmWaiter.code != 0) {
                return SendResult.failed("transfer confirm rejected: code=" + confirmWaiter.code + " " + confirmWaiter.message);
            }
        } finally {
            TRANSFER_CONFIRM_WAITERS.remove(confirmScene);
        }

        int status = queryTransferStatus(classLoader, transfer, groupUsername);
        if ("refuse".equals(operation)) {
            if (status != 2001 && isTerminalTransferStatus(status)) {
                log("transfer declined source_chat_record_id=" + sourceChatRecordId
                        + " amount_minor=" + transfer.amountMinor
                        + " currency=" + transfer.currency
                        + " status=" + status);
                return SendResult.sent(sourceChatRecordId);
            }
            return SendResult.failed("transfer decline was not verified: status=" + status);
        }
        if (status == 2001) {
            log("transfer accepted source_chat_record_id=" + sourceChatRecordId
                    + " amount_minor=" + transfer.amountMinor
                    + " currency=" + transfer.currency);
            return SendResult.sent(sourceChatRecordId);
        }
        return SendResult.failed("transfer confirm was not verified as received: status=" + status);
    }

    /**
     * The transfer confirm/query path never went through {@link #isWeChatReadyForSend},
     * so unlike the text-send path it never triggered the module's existing
     * MMApplicationContext (x2.a) recovery. Root cause per handoff doc 13.26:
     * com.tencent.mm.wallet_core.tenpay.model.o's static init eventually calls
     * x2.a.getString(...), and if x2.a is still null on this device the resulting
     * NullPointerException permanently marks o (and its subclasses n0/h1) erroneous
     * for the process. Reuses the existing, already-proven recovery logic instead of
     * writing new speculative code.
     */
    private static void ensureWeChatRegistriesForTransfer(ClassLoader classLoader) {
        try {
            classLoader = runtimeClassLoader(classLoader);
            ensureWeChatBuildInfo(APP_CONTEXT, classLoader);
            ensureWeChatRegistries(classLoader, weChatSymbols(classLoader));
            ensureWalletPluginInitialized(classLoader);
            ensureTenpaySignFallbackHook(classLoader);
        } catch (Throwable t) {
            log("transfer diag: ensureWeChatRegistries failed: " + fullErrorChain(t));
        }
    }

    /**
     * Diagnostic-only: forces class init on the transfer NetScene base chain,
     * ancestor first, so a class already marked erroneous on this device (from
     * some earlier, unrelated touch) reports its ORIGINAL failure here instead
     * of the generic "sub-type of erroneous class" message the eventual
     * confirmConstructor.newInstance() call would surface. Never throws, never
     * changes the accept flow's return value.
     */
    private static void probeTransferClassChain(ClassLoader classLoader) {
        String[] chain = {
                weChatSymbols(classLoader).className("com.tencent.mm.modelbase.m1"),
                "com.tencent.mm.wallet_core.model.d1",
                "com.tencent.mm.wallet_core.model.z0",
                "com.tencent.mm.wallet_core.tenpay.model.o",
        };
        for (String className : chain) {
            try {
                Class.forName(className, true, classLoader);
            } catch (Throwable t) {
                log("transfer diag: class init failed at " + className + ": " + fullErrorChain(t));
                return;
            }
        }
        log("transfer diag: transfer base class chain initialized OK");
    }

    private static int queryTransferStatus(
            ClassLoader classLoader,
            TransferMessageParser.Result transfer,
            String groupUsername) throws Exception {
        return queryTransferStatus(classLoader, transfer, groupUsername, 3, 20L);
    }

    private static int queryTransferStatus(
            ClassLoader classLoader,
            TransferMessageParser.Result transfer,
            String groupUsername,
            int maxAttempts,
            long timeoutSeconds) throws Exception {
        ensureWeChatRegistriesForTransfer(classLoader);
        Class<?> queryClass = findClass(classLoader, "com.tencent.mm.plugin.remittance.model.h1");
        ensureTransferCallbackHook(queryClass, TRANSFER_QUERY_CALLBACK_HOOKED, TRANSFER_QUERY_WAITERS);
        Constructor<?> queryConstructor = findConstructor(
                queryClass,
                int.class,
                String.class,
                String.class,
                int.class,
                String.class,
                String.class);
        int lastStatus = 0;
        boolean responseObserved = false;
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            Object queryScene = newTransferScene(
                    queryConstructor,
                    1,
                    transfer.transactionId,
                    transfer.transferId,
                    transfer.invalidTime,
                    groupUsername,
                    transfer.attach);
            NetworkResultWaiter queryWaiter = new NetworkResultWaiter();
            TRANSFER_QUERY_WAITERS.put(queryScene, queryWaiter);
            try {
                setNetSceneProcessName(queryScene, "RemittanceProcess");
                enqueueNetScene(classLoader, queryScene);
                long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeoutSeconds);
                while (System.currentTimeMillis() < deadline) {
                    lastStatus = (int) getLongField(queryScene, "h");
                    if (lastStatus != 0) {
                        responseObserved = true;
                        break;
                    }
                    if (queryWaiter.isComplete()) {
                        responseObserved = true;
                        if (queryWaiter.code != 0) {
                            throw new IllegalStateException("transfer status query rejected: code="
                                    + queryWaiter.code + " " + queryWaiter.message);
                        }
                    }
                    Thread.sleep(250L);
                }
                if (queryWaiter.isComplete() && queryWaiter.code != 0) {
                    throw new IllegalStateException("transfer status query rejected: code="
                            + queryWaiter.code + " " + queryWaiter.message);
                }
                lastStatus = (int) getLongField(queryScene, "h");
            } finally {
                TRANSFER_QUERY_WAITERS.remove(queryScene);
            }
            if (isTerminalTransferStatus(lastStatus)) {
                return lastStatus;
            }
            Thread.sleep(750L);
        }
        if (!responseObserved && lastStatus == 0) {
            throw new IllegalStateException("transfer status query timed out");
        }
        return lastStatus;
    }

    private static boolean isTerminalTransferStatus(int status) {
        return status == 2001 || status == 2002 || status == 2003
                || status == 2004 || status == 2005;
    }

    private static void ensureTransferCallbackHook(
            Class<?> sceneClass,
            AtomicBoolean hooked,
            final ConcurrentHashMap<Object, NetworkResultWaiter> waiters) {
        if (!hooked.compareAndSet(false, true)) {
            return;
        }
        try {
            XposedBridge.hookAllMethods(sceneClass, "onGYNetEnd", new XC_MethodHook() {
                @Override
                protected void afterHookedMethod(MethodHookParam param) {
                    NetworkResultWaiter waiter = waiters.get(param.thisObject);
                    if (waiter != null) {
                        waiter.complete(param.args);
                    }
                }
            });
        } catch (Throwable t) {
            hooked.set(false);
            throw new IllegalStateException("hook transfer callback failed", t);
        }
    }

    private static void setNetSceneProcessName(Object scene, String processName) {
        try {
            findMethod(scene.getClass(), "setProcessName", String.class).invoke(scene, processName);
        } catch (Throwable t) {
            log("set transfer NetScene process name skipped: " + shortError(t));
        }
    }

    /**
     * Per handoff doc 13.31.5: ho0.j1.e() depends on a Kernel singleton (ho0.j1.m)
     * that is null in this dual-instance process/classloader, causing "Kernel not
     * initialized by MMApplication!" at enqueue time. 13.32/13.33 switched to
     * com.tencent.mm.modelbase.z2.b(scene) to reuse the entry point sendViaNetScene
     * uses for text -- but per doc 13.36, dexdump of z2.b(m1) shows it forwards to
     * com.tencent.mm.modelbase.r1.d(m1), which is NOT an enqueue method, it is
     * CANCEL (logs "cancel sceneHashCode:%d", calls scene.cancel(), removes it from
     * the running/pending vectors). Every z2.b() call was silently self-cancelling
     * the request before it ever reached the network -- that is why nothing showed
     * up in RunCgi/network logs and every wait timed out. The real enqueue method is
     * r1.h(m1, int) (r1.g(m1) is a same-class convenience wrapper for h(scene, 0)),
     * on the SAME r1 instance z2 already holds a working reference to (confirmed
     * non-null by z2.c()==true): z2's static field b (a ho0.n) -> field a (a ho0.y)
     * -> field b (the com.tencent.mm.modelbase.r1 NetSceneQueue). Reach that same r1
     * instance directly and call the real .h(scene, 0) on it, instead of z2.b().
     */
    private static Object resolveNetSceneQueueInstance(ClassLoader classLoader) throws Exception {
        Class<?> runCgi = findVersionedClass(classLoader, "com.tencent.mm.modelbase.z2");
        Object queueHandle = findField(runCgi, "b").get(null);
        if (queueHandle == null) {
            throw new IllegalStateException("NetSceneQueue is not ready");
        }
        Object y = findField(queueHandle.getClass(), "a").get(queueHandle);
        if (y == null) {
            throw new IllegalStateException("NetSceneQueue handle is incomplete");
        }
        Object netSceneQueue = findField(y.getClass(), "b").get(y);
        if (netSceneQueue == null) {
            throw new IllegalStateException("NetSceneQueue is not ready");
        }
        return netSceneQueue;
    }

    // NetSceneQueue (com.tencent.mm.modelbase.r1 in 8.0.78, s1 in 8.0.79 -- unaliased since
    // this call site resolves the instance structurally via resolveNetSceneQueueInstance
    // rather than by canonical class name) tracks its own app-foreground/background state
    // via the stable, un-obfuscated com.tencent.mm.app.s2 callback interface
    // (onAppForeground/onAppBackground are kept literal across versions, confirmed in both
    // 8.0.78's r1.java and 8.0.79's s1.java) and throttles/defers real network dispatch of
    // queued NetScenes while backgrounded -- independently of, and in addition to, Android's
    // own OS-level background execution limits. This is why a send can locally succeed (the
    // scene is accepted into the queue, our own verification sees the local DB row and a
    // non-negative msgId) yet never actually reach the recipient until WeChat's own UI is
    // reopened and this flips back to foreground, flushing the backlog. Proactively nudging
    // the queue into believing it is foreground immediately before enqueueing sidesteps that
    // throttle without needing the real Activity visible. Best-effort only: never allowed to
    // block or fail the actual send if the nudge itself doesn't work on some version.
    private static void wakeNetSceneQueueForeground(ClassLoader classLoader) {
        try {
            Object netSceneQueue = resolveNetSceneQueueInstance(classLoader);
            Method onAppForeground = findMethod(netSceneQueue.getClass(), "onAppForeground", String.class);
            onAppForeground.invoke(netSceneQueue, "cc.wechat.observatory");
        } catch (Throwable t) {
            log("wakeNetSceneQueueForeground failed: " + shortError(rootCause(t)));
        }
    }

    private static void enqueueNetScene(ClassLoader classLoader, Object scene) throws Exception {
        Object netSceneQueue = resolveNetSceneQueueInstance(classLoader);
        wakeNetSceneQueueForeground(classLoader);
        Method enqueue = findMethod(
                netSceneQueue.getClass(),
                "h",
                findVersionedClass(classLoader, "com.tencent.mm.modelbase.m1"),
                int.class);
        Object accepted = enqueue.invoke(netSceneQueue, scene, 0);
        if (accepted instanceof Boolean && !((Boolean) accepted)) {
            throw new IllegalStateException("NetSceneQueue rejected transfer request");
        }
    }

    private static String outboxKind(JSONObject item) {
        if (item == null) {
            return "text";
        }
        String kind = item.optString("kind", "");
        if (isBlank(kind)) {
            kind = isBlank(item.optString("media_kind", "")) ? "text" : item.optString("media_kind", "");
        }
        kind = kind.trim().toLowerCase(Locale.US);
        return isBlank(kind) ? "text" : kind;
    }

    private static JSONObject outboxAck(long id, SendResult result) throws Exception {
        JSONObject ack = new JSONObject();
        ack.put("id", id);
        ack.put("status", result.ok ? "sent" : "failed");
        if (result.chatRecordId > 0) {
            ack.put("chat_record_id", result.chatRecordId);
        }
        if (!result.ok) {
            ack.put("error", result.error);
        }
        return ack;
    }

    private static SendResult sendImageAction(BridgeConfig config, ClassLoader classLoader, String wxid, JSONObject item) {
        File mediaFile = null;
        try {
            String mediaUrl = firstNonBlank(
                    item.optString("media_url", ""),
                    item.optJSONObject("payload_json") == null ? "" : item.optJSONObject("payload_json").optString("media_url", ""));
            String mediaName = firstNonBlank(
                    item.optString("media_name", ""),
                    item.optJSONObject("payload_json") == null ? "" : item.optJSONObject("payload_json").optString("media_name", ""));
            if (isBlank(mediaUrl)) {
                return SendResult.failed("image media_url is required");
            }
            mediaFile = downloadOutboxMedia(config, mediaUrl, mediaName);
            if (mediaFile == null || !mediaFile.isFile() || mediaFile.length() <= 0) {
                return SendResult.failed("image media download produced empty file");
            }
            SendResult result = sendImage(config, classLoader, wxid, mediaFile, item.optString("text", ""));
            if (result.ok) {
                mediaFile = null;
            }
            return result;
        } catch (Throwable t) {
            return SendResult.failed("image send failed: " + shortError(t));
        } finally {
            if (mediaFile != null && mediaFile.isFile() && !mediaFile.delete()) {
                log("delete outbox image temp file skipped");
            }
        }
    }

    private static SendResult sendVideoAction(BridgeConfig config, ClassLoader classLoader, String wxid, JSONObject item) {
        File mediaFile = null;
        try {
            String mediaUrl = firstNonBlank(
                    item.optString("media_url", ""),
                    item.optJSONObject("payload_json") == null ? "" : item.optJSONObject("payload_json").optString("media_url", ""));
            String mediaName = firstNonBlank(
                    item.optString("media_name", ""),
                    item.optJSONObject("payload_json") == null ? "" : item.optJSONObject("payload_json").optString("media_name", ""));
            if (isBlank(mediaUrl)) {
                return SendResult.failed("video media_url is required");
            }
            mediaFile = downloadOutboxMedia(config, mediaUrl, mediaName);
            if (mediaFile == null || !mediaFile.isFile() || mediaFile.length() <= 0) {
                return SendResult.failed("video media download produced empty file");
            }
            SendResult result = sendVideo(config, classLoader, wxid, mediaFile);
            if (result.ok) {
                mediaFile = null;
            }
            return result;
        } catch (Throwable t) {
            return SendResult.failed("video send failed: " + shortError(t));
        } finally {
            if (mediaFile != null && mediaFile.isFile() && !mediaFile.delete()) {
                log("delete outbox video temp file skipped");
            }
        }
    }

    private static SendResult sendVoiceAction(BridgeConfig config, ClassLoader classLoader, String wxid, JSONObject item) {
        File mediaFile = null;
        try {
            JSONObject payload = item.optJSONObject("payload_json");
            String mediaUrl = firstNonBlank(
                    item.optString("media_url", ""),
                    payload == null ? "" : payload.optString("media_url", ""));
            String mediaName = firstNonBlank(
                    item.optString("media_name", ""),
                    payload == null ? "" : payload.optString("media_name", ""));
            int durationMs = firstPositiveInt(
                    item.optInt("media_duration_ms", 0),
                    item.optInt("duration_ms", 0),
                    payload == null ? 0 : payload.optInt("media_duration_ms", 0),
                    payload == null ? 0 : payload.optInt("duration_ms", 0),
                    1000);
            if (isBlank(mediaUrl)) {
                return SendResult.failed("voice media_url is required");
            }
            mediaFile = downloadOutboxMedia(config, mediaUrl, mediaName);
            if (mediaFile == null || !mediaFile.isFile() || mediaFile.length() <= 0) {
                return SendResult.failed("voice media download produced empty file");
            }
            if (!isSupportedVoiceMedia(mediaFile, mediaName)) {
                return SendResult.failed("voice media must be AMR or SILK");
            }
            SendResult result = sendVoice(config, classLoader, wxid, mediaFile, durationMs);
            if (result.ok) {
                mediaFile = null;
            }
            return result;
        } catch (Throwable t) {
            return SendResult.failed("voice send failed: " + shortError(t));
        } finally {
            if (mediaFile != null && mediaFile.isFile() && !mediaFile.delete()) {
                log("delete outbox voice temp file skipped");
            }
        }
    }

    private static SendResult sendFileAction(BridgeConfig config, ClassLoader classLoader, String wxid, JSONObject item) {
        File mediaFile = null;
        try {
            String mediaUrl = firstNonBlank(
                    item.optString("media_url", ""),
                    item.optJSONObject("payload_json") == null ? "" : item.optJSONObject("payload_json").optString("media_url", ""));
            String mediaName = firstNonBlank(
                    item.optString("media_name", ""),
                    item.optJSONObject("payload_json") == null ? "" : item.optJSONObject("payload_json").optString("media_name", ""));
            if (isBlank(mediaUrl)) {
                return SendResult.failed("file media_url is required");
            }
            mediaFile = downloadOutboxMedia(config, mediaUrl, mediaName, true);
            if (mediaFile == null || !mediaFile.isFile() || mediaFile.length() <= 0) {
                return SendResult.failed("file media download produced empty file");
            }
            SendResult result = sendFile(config, classLoader, wxid, mediaFile);
            if (result.ok) {
                mediaFile = null;
            }
            return result;
        } catch (Throwable t) {
            return SendResult.failed("file send failed: " + shortError(t));
        } finally {
            if (mediaFile != null && mediaFile.isFile() && !mediaFile.delete()) {
                log("delete outbox file temp file skipped");
            }
        }
    }

    private static SendResult sendEmojiAction(BridgeConfig config, ClassLoader classLoader, String wxid, JSONObject item) {
        try {
            JSONObject payload = item.optJSONObject("payload_json");
            long sourceChatRecordId = firstPositiveLong(
                    item.optLong("source_chat_record_id", 0L),
                    payload == null ? 0L : payload.optLong("source_chat_record_id", 0L));
            String emojiMd5 = firstNonBlank(
                    item.optString("emoji_md5", ""),
                    payload == null ? "" : payload.optString("emoji_md5", ""));
            if (isBlank(emojiMd5) && sourceChatRecordId > 0L) {
                Object db = ensureMessageDatabase(config);
                if (db == null) {
                    return SendResult.failed("WeChat message database is not available for emoji source lookup");
                }
                ChatHistorySource source = loadChatHistorySource(db, sourceChatRecordId);
                if (source == null) {
                    return SendResult.failed("source emoji message was not found in local WeChat message database");
                }
                if (source.type != 47 && !(source.type == MESSAGE_TYPE_APPMSG && appMsgTypeFromContent(source.content) == 8)) {
                    return SendResult.failed("source_chat_record_id is not an emoji message");
                }
                emojiMd5 = emojiMd5FromContent(source.talker, source.content);
            }
            if (isBlank(emojiMd5)) {
                return SendResult.failed("emoji_md5 or source_chat_record_id is required");
            }
            return sendEmoji(config, classLoader, wxid, emojiMd5);
        } catch (Throwable t) {
            return SendResult.failed("emoji send failed: " + shortError(t));
        }
    }

    private static SendResult sendLocationAction(BridgeConfig config, ClassLoader classLoader, String wxid, String text, JSONObject item) {
        try {
            JSONObject payload = item.optJSONObject("payload_json");
            double latitude = firstFiniteDouble(
                    item.optDouble("location_latitude", Double.NaN),
                    item.optDouble("latitude", Double.NaN),
                    payload == null ? Double.NaN : payload.optDouble("location_latitude", Double.NaN),
                    payload == null ? Double.NaN : payload.optDouble("latitude", Double.NaN));
            double longitude = firstFiniteDouble(
                    item.optDouble("location_longitude", Double.NaN),
                    item.optDouble("longitude", Double.NaN),
                    payload == null ? Double.NaN : payload.optDouble("location_longitude", Double.NaN),
                    payload == null ? Double.NaN : payload.optDouble("longitude", Double.NaN));
            int scale = firstPositiveInt(
                    item.optInt("location_scale", 0),
                    item.optInt("scale", 0),
                    payload == null ? 0 : payload.optInt("location_scale", 0),
                    payload == null ? 0 : payload.optInt("scale", 0),
                    16);
            String label = firstNonBlank(
                    item.optString("location_label", ""),
                    payload == null ? "" : payload.optString("location_label", ""),
                    text,
                    "[位置]");
            String poiName = firstNonBlank(
                    item.optString("location_poiname", ""),
                    item.optString("location_poi_name", ""),
                    payload == null ? "" : payload.optString("location_poiname", ""),
                    payload == null ? "" : payload.optString("location_poi_name", ""),
                    label);
            String infoURL = firstNonBlank(
                    item.optString("location_info_url", ""),
                    payload == null ? "" : payload.optString("location_info_url", ""));
            String poiID = firstNonBlank(
                    item.optString("location_poi_id", ""),
                    payload == null ? "" : payload.optString("location_poi_id", ""));
            String poiTips = firstNonBlank(
                    item.optString("location_poi_category_tips", ""),
                    payload == null ? "" : payload.optString("location_poi_category_tips", ""));
            boolean fromPoiList = item.optBoolean(
                    "location_from_poi_list",
                    payload != null && payload.optBoolean("location_from_poi_list", false));
            return sendLocation(config, classLoader, wxid, latitude, longitude, scale, label, poiName, infoURL, poiID, fromPoiList, poiTips);
        } catch (Throwable t) {
            return SendResult.failed("location send failed: " + shortError(t));
        }
    }

    private static SendResult sendQuoteAction(BridgeConfig config, ClassLoader classLoader, String wxid, String text, JSONObject item) {
        try {
            JSONObject payload = item.optJSONObject("payload_json");
            long quoteMsgId = firstPositiveLong(
                    item.optLong("quote_msg_id", 0L),
                    item.optLong("quote_chat_record_id", 0L),
                    payload == null ? 0L : payload.optLong("quote_msg_id", 0L),
                    payload == null ? 0L : payload.optLong("quote_chat_record_id", 0L));
            String quoteTalker = firstNonBlank(
                    item.optString("quote_talker", ""),
                    payload == null ? "" : payload.optString("quote_talker", ""),
                    wxid);
            String quoteSenderWxid = firstNonBlank(
                    item.optString("quote_sender_wxid", ""),
                    payload == null ? "" : payload.optString("quote_sender_wxid", ""));
            if (isBlank(text)) {
                return SendResult.failed("quote text is required");
            }
            if (quoteMsgId <= 0) {
                return SendResult.failed("quote_msg_id is required");
            }
            return sendQuote(config, classLoader, wxid, text, quoteMsgId, quoteTalker, quoteSenderWxid);
        } catch (Throwable t) {
            return SendResult.failed("quote send failed: " + shortError(t));
        }
    }

    private static SendResult sendLinkAction(BridgeConfig config, ClassLoader classLoader, String wxid, String text, JSONObject item) {
        try {
            JSONObject payload = item.optJSONObject("payload_json");
            long sourceChatRecordId = firstPositiveLong(
                    item.optLong("source_chat_record_id", 0L),
                    payload == null ? 0L : payload.optLong("source_chat_record_id", 0L));
            if (sourceChatRecordId > 0L) {
                return sendSourceAppMsg(config, classLoader, wxid, sourceChatRecordId, "link", APPMSG_TYPE_LINK);
            }
            String title = firstNonBlank(
                    item.optString("appmsg_title", ""),
                    payload == null ? "" : payload.optString("appmsg_title", ""),
                    text);
            String description = firstNonBlank(
                    item.optString("appmsg_description", ""),
                    payload == null ? "" : payload.optString("appmsg_description", ""));
            String url = firstNonBlank(
                    item.optString("appmsg_url", ""),
                    payload == null ? "" : payload.optString("appmsg_url", ""));
            String appName = firstNonBlank(
                    item.optString("appmsg_app_name", ""),
                    payload == null ? "" : payload.optString("appmsg_app_name", ""));
            String thumbUrl = firstNonBlank(
                    item.optString("appmsg_thumb_url", ""),
                    payload == null ? "" : payload.optString("appmsg_thumb_url", ""));
            if (isBlank(title)) {
                return SendResult.failed("appmsg_title is required");
            }
            if (isBlank(url)) {
                return SendResult.failed("appmsg_url is required");
            }
            return sendLink(config, classLoader, wxid, title, description, url, appName, thumbUrl);
        } catch (Throwable t) {
            return SendResult.failed("link send failed: " + shortError(t));
        }
    }

    private static SendResult sendRevokeAction(BridgeConfig config, ClassLoader classLoader, String wxid, JSONObject item) {
        try {
            JSONObject payload = item.optJSONObject("payload_json");
            long chatRecordId = firstPositiveLong(
                    item.optLong("chat_record_id", 0L),
                    payload == null ? 0L : payload.optLong("chat_record_id", 0L),
                    item.optLong("source_chat_record_id", 0L),
                    payload == null ? 0L : payload.optLong("source_chat_record_id", 0L));
            if (chatRecordId <= 0L) {
                return SendResult.failed("chat_record_id is required");
            }
            return sendRevoke(config, classLoader, wxid, chatRecordId);
        } catch (Throwable t) {
            return SendResult.failed("revoke send failed: " + shortError(t));
        }
    }

    private static SendResult sendMiniProgramAction(BridgeConfig config, ClassLoader classLoader, String wxid, String text, JSONObject item) {
        try {
            JSONObject payload = item.optJSONObject("payload_json");
            long sourceChatRecordId = firstPositiveLong(
                    item.optLong("source_chat_record_id", 0L),
                    payload == null ? 0L : payload.optLong("source_chat_record_id", 0L));
            if (sourceChatRecordId > 0L) {
                return sendSourceAppMsg(config, classLoader, wxid, sourceChatRecordId, "mini_program",
                        APPMSG_TYPE_MINI_PROGRAM, APPMSG_TYPE_MINI_PROGRAM_LEGACY);
            }
            String title = firstNonBlank(
                    item.optString("appmsg_title", ""),
                    payload == null ? "" : payload.optString("appmsg_title", ""),
                    text);
            String description = firstNonBlank(
                    item.optString("appmsg_description", ""),
                    payload == null ? "" : payload.optString("appmsg_description", ""));
            String url = firstNonBlank(
                    item.optString("appmsg_url", ""),
                    payload == null ? "" : payload.optString("appmsg_url", ""));
            String appName = firstNonBlank(
                    item.optString("appmsg_app_name", ""),
                    payload == null ? "" : payload.optString("appmsg_app_name", ""));
            String username = firstNonBlank(
                    item.optString("mini_program_username", ""),
                    payload == null ? "" : payload.optString("mini_program_username", ""));
            String pagePath = firstNonBlank(
                    item.optString("mini_program_page_path", ""),
                    payload == null ? "" : payload.optString("mini_program_page_path", ""));
            String appId = firstNonBlank(
                    item.optString("mini_program_appid", ""),
                    payload == null ? "" : payload.optString("mini_program_appid", ""));
            String iconUrl = firstNonBlank(
                    item.optString("mini_program_icon_url", ""),
                    payload == null ? "" : payload.optString("mini_program_icon_url", ""),
                    item.optString("appmsg_thumb_url", ""),
                    payload == null ? "" : payload.optString("appmsg_thumb_url", ""));
            int version = firstPositiveInt(
                    item.optInt("mini_program_version", 0),
                    payload == null ? 0 : payload.optInt("mini_program_version", 0));
            int miniType = firstPositiveInt(
                    item.optInt("mini_program_type", 0),
                    payload == null ? 0 : payload.optInt("mini_program_type", 0));
            if (isBlank(title)) {
                return SendResult.failed("appmsg_title is required");
            }
            if (isBlank(username)) {
                return SendResult.failed("mini_program_username is required");
            }
            if (isBlank(pagePath)) {
                return SendResult.failed("mini_program_page_path is required");
            }
            return sendMiniProgram(config, classLoader, wxid, title, description, url, appName, username, pagePath, appId, iconUrl, version, miniType);
        } catch (Throwable t) {
            return SendResult.failed("mini_program send failed: " + shortError(t));
        }
    }

    private static SendResult sendChatHistoryAction(BridgeConfig config, ClassLoader classLoader, String wxid, String text, JSONObject item) {
        try {
            JSONObject payload = item.optJSONObject("payload_json");
            String title = firstNonBlank(
                    item.optString("record_title", ""),
                    payload == null ? "" : payload.optString("record_title", ""),
                    text,
                    "聊天记录");
            String description = firstNonBlank(
                    item.optString("record_description", ""),
                    payload == null ? "" : payload.optString("record_description", ""));
            String recordItemXML = firstNonBlank(
                    item.optString("recorditem_xml", ""),
                    payload == null ? "" : payload.optString("recorditem_xml", ""),
                    payload == null ? "" : payload.optString("record_xml", ""));
            recordItemXML = normalizeRecordItemXML(recordItemXML);
            List<Long> sourceChatRecordIds = readSourceChatRecordIds(
                    item.optJSONArray("source_chat_record_ids"),
                    payload == null ? null : payload.optJSONArray("source_chat_record_ids"));
            boolean forwardOriginal = item.optBoolean("forward_original", payload != null && payload.optBoolean("forward_original", false));
            long sourceChatRecordId = firstPositiveLong(
                    item.optLong("source_chat_record_id", 0L),
                    payload == null ? 0L : payload.optLong("source_chat_record_id", 0L),
                    sourceChatRecordIds.isEmpty() ? 0L : sourceChatRecordIds.get(0).longValue());
            if (forwardOriginal) {
                if (sourceChatRecordId <= 0L) {
                    return SendResult.failed("source_chat_record_id is required when forward_original is true");
                }
                Object db = ensureMessageDatabase(config);
                if (db == null) {
                    return SendResult.failed("WeChat message database is not available for original chat_history forward");
                }
                ChatHistorySource source = loadChatHistorySource(db, sourceChatRecordId);
                if (source == null) {
                    return SendResult.failed("source chat_history message was not found in local WeChat message database");
                }
                String sourceContent = normalizeMessageText(source.talker, source.content);
                if (source.type != MESSAGE_TYPE_APPMSG || appMsgTypeFromContent(sourceContent) != APPMSG_TYPE_CHAT_HISTORY) {
                    return SendResult.failed("source_chat_record_id must point to a chat_history appmsg");
                }
                String originalRecordItemXML = normalizeRecordItemXML(extractXmlFieldRaw(sourceContent, "recorditem"));
                if (isBlank(originalRecordItemXML)) {
                    return SendResult.failed("source chat_history recorditem is missing");
                }
                if (!originalRecordItemXML.trim().startsWith("<recordinfo")) {
                    return SendResult.failed("source chat_history recorditem must contain recordinfo XML");
                }
                String originalTitle = firstNonBlank(
                        extractXmlField(originalRecordItemXML, "title"),
                        extractXmlField(sourceContent, "title"));
                String originalDescription = firstNonBlank(
                        extractXmlField(sourceContent, "des"),
                        extractXmlField(sourceContent, "desc"),
                        extractXmlField(originalRecordItemXML, "desc"));
                title = firstNonBlank(
                        item.optString("record_title", ""),
                        payload == null ? "" : payload.optString("record_title", ""),
                        originalTitle,
                        text,
                        "聊天记录");
                description = firstNonBlank(
                        item.optString("record_description", ""),
                        payload == null ? "" : payload.optString("record_description", ""),
                        originalDescription);
                return sendChatHistory(config, classLoader, wxid, title, description, originalRecordItemXML);
            }
            if (isBlank(recordItemXML) && !sourceChatRecordIds.isEmpty()) {
                Object db = ensureMessageDatabase(config);
                if (db == null) {
                    return SendResult.failed("WeChat message database is not available for chat_history source build");
                }
                recordItemXML = buildChatHistoryRecordItemXML(db, sourceChatRecordIds);
                if (isBlank(description)) {
                    description = "共" + sourceChatRecordIds.size() + "条";
                }
            }
            if (isBlank(recordItemXML)) {
                return SendResult.failed("recorditem_xml or source_chat_record_ids is required");
            }
            if (!recordItemXML.trim().startsWith("<recordinfo")) {
                return SendResult.failed("recorditem_xml must contain recordinfo XML");
            }
            return sendChatHistory(config, classLoader, wxid, title, description, recordItemXML);
        } catch (Throwable t) {
            return SendResult.failed("chat_history send failed: " + shortError(t));
        }
    }

    private static File downloadOutboxMedia(BridgeConfig config, String mediaUrl, String mediaName) throws Exception {
        return downloadOutboxMedia(config, mediaUrl, mediaName, false);
    }

    private static File downloadOutboxMedia(BridgeConfig config, String mediaUrl, String mediaName, boolean preserveName) throws Exception {
        if (config == null || isBlank(config.baseUrl) || isBlank(config.apiKey)) {
            throw new IOException("bridge config is missing");
        }
        String path = mediaUrl.trim();
        if (path.startsWith("/api/media/")) {
            path = "/module/media/" + path.substring("/api/media/".length());
        }
        if (!path.startsWith("/module/media/")) {
            throw new IOException("unsupported media_url");
        }
        String separator = path.indexOf('?') >= 0 ? "&" : "?";
        URL url = new URL(trimRight(config.baseUrl, "/") + path + separator + "api_key=" + urlEncode(config.apiKey));
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(8000);
        connection.setReadTimeout(15000);
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Connection", "close");
        int status = connection.getResponseCode();
        if (status < 200 || status >= 300) {
            String response = readResponse(connection.getErrorStream());
            connection.disconnect();
            throw new IOException("media download HTTP " + status + ": " + response);
        }
        File dir = outboxMediaDir();
        if (!dir.isDirectory() && !dir.mkdirs()) {
            connection.disconnect();
            throw new IOException("create outbox media dir failed");
        }
        String suffix = preserveName
                ? "-" + safeCacheFileName(mediaName, path)
                : safeCacheExtension(mediaName, path);
        File target = new File(dir, "outbox-" + System.currentTimeMillis() + suffix);
        long total = 0L;
        byte[] buffer = new byte[8192];
        try (InputStream input = connection.getInputStream(); FileOutputStream output = new FileOutputStream(target, false)) {
            int read;
            long limit = Math.max(OUTBOX_MEDIA_DOWNLOAD_LIMIT_BYTES, config.mediaUploadLimitBytes);
            while ((read = input.read(buffer)) != -1) {
                total += read;
                if (total > limit) {
                    throw new IOException("media download exceeds limit");
                }
                output.write(buffer, 0, read);
            }
        } finally {
            connection.disconnect();
        }
        return target;
    }

    private static File outboxMediaDir() throws IOException {
        Context context = APP_CONTEXT;
        if (context == null) {
            Object app = currentApplication();
            if (app instanceof Context) {
                context = (Context) app;
            }
        }
        if (context == null || context.getCacheDir() == null) {
            throw new IOException("cache dir is not available");
        }
        return new File(context.getCacheDir(), "wechat-observatory-outbox");
    }

    private static String safeCacheExtension(String mediaName, String mediaUrl) {
        String value = firstNonBlank(mediaName, mediaUrl).trim().toLowerCase(Locale.US);
        int query = value.indexOf('?');
        if (query >= 0) {
            value = value.substring(0, query);
        }
        int dot = value.lastIndexOf('.');
        if (dot < 0 || dot + 1 >= value.length()) {
            return ".img";
        }
        String ext = value.substring(dot);
        if (ext.length() > 8) {
            return ".img";
        }
        for (int i = 1; i < ext.length(); i++) {
            char ch = ext.charAt(i);
            if (!((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9'))) {
                return ".img";
            }
        }
        return ext;
    }

    private static String safeCacheFileName(String mediaName, String mediaUrl) {
        String value = firstNonBlank(mediaName, mediaUrl).trim();
        int query = value.indexOf('?');
        if (query >= 0) {
            value = value.substring(0, query);
        }
        int slash = Math.max(value.lastIndexOf('/'), value.lastIndexOf('\\'));
        if (slash >= 0 && slash + 1 < value.length()) {
            value = value.substring(slash + 1);
        }
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < value.length() && out.length() < 80; i++) {
            char ch = value.charAt(i);
            if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9')) {
                out.append(ch);
            } else if (ch == '.' || ch == '_' || ch == '-') {
                out.append(ch);
            }
        }
        String name = out.toString();
        if (isBlank(name) || ".".equals(name) || "..".equals(name) || name.startsWith(".")) {
            String ext = safeCacheExtension(mediaName, mediaUrl);
            if (".img".equals(ext)) {
                ext = ".bin";
            }
            name = "file" + ext;
        }
        return name;
    }

    private static boolean isSupportedVoiceMedia(File file, String mediaName) {
        String name = firstNonBlank(mediaName, file == null ? "" : file.getName()).trim().toLowerCase(Locale.US);
        if (name.endsWith(".amr") || name.endsWith(".silk")) {
            return true;
        }
        if (file == null || !file.isFile()) {
            return false;
        }
        byte[] header = new byte[16];
        try (FileInputStream input = new FileInputStream(file)) {
            int read = input.read(header);
            if (read <= 0) {
                return false;
            }
            String head = new String(header, 0, read, StandardCharsets.US_ASCII);
            return head.startsWith("#!AMR") || head.startsWith("#!SILK");
        } catch (Throwable t) {
            log("voice media header check failed: " + shortError(t));
            return false;
        }
    }

    private static String prepareWeChatVoiceFile(ClassLoader classLoader, String voiceFileName, File source) throws Exception {
        // audioScoped=true used to be the only path this required (the audioScoped=false
        // "legacy" lookup was always best-effort, wrapped in its own try/catch below). On
        // 8.0.79 the audioScoped=true call still resolves to the right class/method but
        // comes back empty from some rewritten internal logic we can't single-step through
        // (its body doesn't decompile and none of the stable utility methods it used to call
        // on 8.0.78 get hit anymore). Rather than hard-fail here, try both variants and use
        // whichever actually returns a path -- audioScoped=true preferred when it works, but
        // audioScoped=false is no less valid a storage location if it's the only one that
        // resolves.
        String audioPath = "";
        Throwable audioError = null;
        try {
            audioPath = voiceStoragePath(classLoader, voiceFileName, true);
        } catch (Throwable t) {
            audioError = t;
        }
        String legacyPath = "";
        try {
            legacyPath = voiceStoragePath(classLoader, voiceFileName, false);
        } catch (Throwable t) {
            log("prepare legacy voice path failed: " + shortError(t));
        }
        if (isBlank(audioPath) && isBlank(legacyPath)) {
            throw audioError != null
                    ? new IOException("voice storage path is empty", audioError)
                    : new IOException("voice storage path is empty");
        }
        String primaryPath = !isBlank(audioPath) ? audioPath : legacyPath;
        copyFileToWeChatPath(classLoader, source, primaryPath);
        if (!isBlank(legacyPath) && !legacyPath.equals(primaryPath)) {
            try {
                copyFileToWeChatPath(classLoader, source, legacyPath);
            } catch (Throwable t) {
                log("copy to legacy voice path failed: " + shortError(t));
            }
        }
        return primaryPath;
    }

    private static String voiceStoragePath(ClassLoader classLoader, String voiceFileName, boolean audioScoped) throws Exception {
        WeChatVersionSymbols.Profile symbols = weChatSymbols(classLoader);
        Class<?> storageInterface = findVersionedClass(classLoader, "tg3.u0");
        Object storage = findMethod(findVersionedClass(classLoader, "i95.n0"), "c", Class.class).invoke(null, storageInterface);
        if (storage == null) {
            throw new IllegalStateException("voice storage service is not available");
        }
        Class<?> yClass = findVersionedClass(classLoader, "bm5.y");
        Object voiceResource;
        if (audioScoped) {
            Object builder = findField(yClass, "i").get(null);
            Class<?> oi3Class = findVersionedClass(classLoader, "oi3.g");
            Class<?> f0Class = findVersionedClass(classLoader, "bm5.f0");
            Object audioType = findField(f0Class, "u").get(null);
            voiceResource = findMethod(builder.getClass(), "d", oi3Class, f0Class).invoke(builder, null, audioType);
        } else {
            voiceResource = findField(yClass, "j").get(null);
        }
        String pathMethod = symbols.methods.voiceStoragePath;
        Method fullPath = findMethod(storage.getClass(), pathMethod, yClass, String.class, boolean.class);
        Object path = fullPath.invoke(storage, voiceResource, voiceFileName, true);
        String value = path == null ? "" : String.valueOf(path);
        if (isBlank(value)) {
            log("voiceStoragePath diagnostic: storageClass=" + storage.getClass().getName()
                    + " pathMethod=" + pathMethod
                    + " voiceFileName=" + voiceFileName
                    + " audioScoped=" + audioScoped
                    + " voiceResourceClass=" + (voiceResource == null ? "null" : voiceResource.getClass().getName())
                    + " voiceResource=" + voiceResource);
            throw new IOException("voice storage path is empty");
        }
        return value;
    }

    private static void copyFileToWeChatPath(ClassLoader classLoader, File source, String targetPath) throws Exception {
        if (source == null || !source.isFile()) {
            throw new IOException("voice source file is missing");
        }
        if (isBlank(targetPath)) {
            throw new IOException("voice target path is empty");
        }
        Throwable vfsFailure = null;
        try {
            Class<?> vfsClass = findVersionedClass(classLoader, "com.tencent.mm.vfs.w6");
            Method open = findMethod(vfsClass, "B", String.class, boolean.class);
            Object raf = open.invoke(null, targetPath, true);
            if (raf instanceof java.io.RandomAccessFile) {
                try (java.io.RandomAccessFile output = (java.io.RandomAccessFile) raf;
                     FileInputStream input = new FileInputStream(source)) {
                    output.setLength(0L);
                    byte[] buffer = new byte[8192];
                    int read;
                    while ((read = input.read(buffer)) != -1) {
                        output.write(buffer, 0, read);
                    }
                    return;
                }
            }
        } catch (Throwable t) {
            vfsFailure = t;
        }

        try {
            File target = new File(targetPath);
            File parent = target.getParentFile();
            if (parent != null && !parent.isDirectory() && !parent.mkdirs()) {
                throw new IOException("create voice target dir failed");
            }
            try (FileInputStream input = new FileInputStream(source);
                 FileOutputStream output = new FileOutputStream(target, false)) {
                byte[] buffer = new byte[8192];
                int read;
                while ((read = input.read(buffer)) != -1) {
                    output.write(buffer, 0, read);
                }
            }
        } catch (Throwable fileFailure) {
            if (vfsFailure != null) {
                throw new IOException("copy voice file failed via vfs: " + shortError(vfsFailure) + "; file: " + shortError(fileFailure));
            }
            throw fileFailure;
        }
    }

    private static SendResult sendImage(BridgeConfig config, ClassLoader classLoader, String wxid, File imageFile, String text) {
        classLoader = runtimeClassLoader(classLoader);
        if (classLoader == null) {
            return SendResult.failed("WeChat classLoader is not available");
        }
        if (isBlank(wxid) || imageFile == null || !imageFile.isFile()) {
            return SendResult.failed("wxid and image file are required");
        }
        Object db = ensureMessageDatabase(config);
        if (db == null) {
            return SendResult.failed("WeChat message database is not available for image send verification");
        }
        final ClassLoader targetClassLoader = classLoader;
        final String targetWxid = wxid;
        final File targetImageFile = imageFile;
        final long beforeMsgId;
        try {
            beforeMsgId = readMaxMessageId(db);
        } catch (Throwable t) {
            return SendResult.failed("image send verification failed before send: " + shortError(t));
        }
        try {
            callOnMainThread(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    sendViaSendImgMgr(targetClassLoader, targetWxid, targetImageFile);
                    return null;
                }
            });
        } catch (Throwable t) {
            return SendResult.failed("WeChat image send failed via dk5.s5.Vi: " + shortError(t));
        }

        long msgId = waitForOutgoingImageMessage(config, targetWxid, beforeMsgId, 8000L);
        if (msgId > 0) {
            log("sendImage verified outgoing image msgId=" + msgId);
            return SendResult.sent(msgId);
        }
        return SendResult.failed("WeChat image send invoked but no outgoing image message was recorded");
    }

    private static SendResult sendVideo(BridgeConfig config, ClassLoader classLoader, String wxid, File videoFile) {
        classLoader = runtimeClassLoader(classLoader);
        if (classLoader == null) {
            return SendResult.failed("WeChat classLoader is not available");
        }
        if (isBlank(wxid) || videoFile == null || !videoFile.isFile()) {
            return SendResult.failed("wxid and video file are required");
        }
        Object db = ensureMessageDatabase(config);
        if (db == null) {
            return SendResult.failed("WeChat message database is not available for video send verification");
        }
        final ClassLoader targetClassLoader = classLoader;
        final String targetWxid = wxid;
        final File targetVideoFile = videoFile;
        final long beforeMsgId;
        try {
            beforeMsgId = readMaxMessageId(db);
        } catch (Throwable t) {
            return SendResult.failed("video send verification failed before send: " + shortError(t));
        }
        try {
            callOnMainThread(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    sendViaVideoSendFeature(targetClassLoader, targetWxid, targetVideoFile);
                    return null;
                }
            });
        } catch (Throwable t) {
            return SendResult.failed("WeChat video send failed via video feature: " + shortError(t));
        }

        long msgId = waitForOutgoingMediaMessage(config, targetWxid, beforeMsgId, 8000L, "video", "43,62");
        if (msgId > 0) {
            log("sendVideo verified outgoing video msgId=" + msgId);
            return SendResult.sent(msgId);
        }
        return SendResult.failed("WeChat video send invoked but no outgoing video message was recorded");
    }

    private static SendResult sendVoice(BridgeConfig config, ClassLoader classLoader, String wxid, File voiceFile, int durationMs) {
        classLoader = runtimeClassLoader(classLoader);
        if (classLoader == null) {
            return SendResult.failed("WeChat classLoader is not available");
        }
        if (isBlank(wxid) || voiceFile == null || !voiceFile.isFile()) {
            return SendResult.failed("wxid and voice file are required");
        }
        Object db = ensureMessageDatabase(config);
        if (db == null) {
            return SendResult.failed("WeChat message database is not available for voice send verification");
        }
        final ClassLoader targetClassLoader = classLoader;
        final String targetWxid = wxid;
        final File targetVoiceFile = voiceFile;
        final int targetDurationMs = Math.max(1000, durationMs);
        final long beforeMsgId;
        try {
            beforeMsgId = readMaxMessageId(db);
        } catch (Throwable t) {
            return SendResult.failed("voice send verification failed before send: " + shortError(t));
        }
        try {
            callOnMainThread(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    sendViaVoiceSendTask(targetClassLoader, targetWxid, targetVoiceFile, targetDurationMs);
                    return null;
                }
            });
        } catch (Throwable t) {
            return SendResult.failed("WeChat voice send failed via VoiceMsgSendTask: " + shortError(t));
        }

        long msgId = waitForOutgoingMediaMessage(config, targetWxid, beforeMsgId, 12000L, "voice", "34");
        if (msgId > 0) {
            log("sendVoice verified outgoing voice msgId=" + msgId);
            return SendResult.sent(msgId);
        }
        return SendResult.failed("WeChat voice send invoked but no outgoing voice message was recorded");
    }

    private static SendResult sendFile(BridgeConfig config, ClassLoader classLoader, String wxid, File file) {
        classLoader = runtimeClassLoader(classLoader);
        if (classLoader == null) {
            return SendResult.failed("WeChat classLoader is not available");
        }
        if (isBlank(wxid) || file == null || !file.isFile()) {
            return SendResult.failed("wxid and file are required");
        }
        Object db = ensureMessageDatabase(config);
        if (db == null) {
            return SendResult.failed("WeChat message database is not available for file send verification");
        }
        final ClassLoader targetClassLoader = classLoader;
        final String targetWxid = wxid;
        final File targetFile = file;
        final long beforeMsgId;
        try {
            beforeMsgId = readMaxMessageId(db);
        } catch (Throwable t) {
            return SendResult.failed("file send verification failed before send: " + shortError(t));
        }
        try {
            callOnMainThread(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    sendViaFileSendLogic(targetClassLoader, targetWxid, targetFile);
                    return null;
                }
            });
        } catch (Throwable t) {
            return SendResult.failed("WeChat file send failed via FileSendLogic: " + shortError(t));
        }

        long msgId = waitForOutgoingMediaMessage(config, targetWxid, beforeMsgId, 12000L, "file", String.valueOf(MESSAGE_TYPE_FILE_TRANSFER));
        if (msgId <= 0L) {
            msgId = waitForOutgoingAppMsgMessage(config, targetWxid, beforeMsgId, 2000L, "file", APPMSG_TYPE_FILE);
        }
        if (msgId > 0) {
            postVerifiedOutgoingMessage(config, db, msgId, "file");
            log("sendFile verified outgoing file msgId=" + msgId);
            return SendResult.sent(msgId);
        }
        return SendResult.failed("WeChat file send invoked but no outgoing file message was recorded");
    }

    private static SendResult sendEmoji(BridgeConfig config, ClassLoader classLoader, String wxid, String emojiMd5) {
        classLoader = runtimeClassLoader(classLoader);
        if (classLoader == null) {
            return SendResult.failed("WeChat classLoader is not available");
        }
        if (isBlank(wxid) || isBlank(emojiMd5)) {
            return SendResult.failed("wxid and emoji_md5 are required");
        }
        Object db = ensureMessageDatabase(config);
        if (db == null) {
            return SendResult.failed("WeChat message database is not available for emoji send verification");
        }
        final ClassLoader targetClassLoader = classLoader;
        final String targetWxid = wxid;
        final String targetEmojiMd5 = emojiMd5.trim();
        final long beforeMsgId;
        try {
            beforeMsgId = readMaxMessageId(db);
        } catch (Throwable t) {
            return SendResult.failed("emoji send verification failed before send: " + shortError(t));
        }
        try {
            callOnMainThread(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    sendViaEmojiMgr(targetClassLoader, targetWxid, targetEmojiMd5);
                    return null;
                }
            });
        } catch (Throwable t) {
            return SendResult.failed("WeChat emoji send failed via EmojiMgrImpl: " + shortError(t));
        }

        long msgId = waitForOutgoingMediaMessage(config, targetWxid, beforeMsgId, 8000L, "emoji", "47");
        if (msgId > 0L) {
            log("sendEmoji verified outgoing emoji msgId=" + msgId);
            return SendResult.sent(msgId);
        }
        return SendResult.failed("WeChat emoji send invoked but no outgoing emoji message was recorded");
    }

    private static SendResult sendLocation(BridgeConfig config, ClassLoader classLoader, String wxid, double latitude, double longitude, int scale, String label, String poiName, String infoURL, String poiID, boolean fromPoiList, String poiTips) {
        classLoader = runtimeClassLoader(classLoader);
        if (classLoader == null) {
            return SendResult.failed("WeChat classLoader is not available");
        }
        if (isBlank(wxid) || !isFinite(latitude) || !isFinite(longitude)) {
            return SendResult.failed("wxid, location_latitude, and location_longitude are required");
        }
        if (latitude < -90D || latitude > 90D || longitude < -180D || longitude > 180D) {
            return SendResult.failed("location coordinates are out of range");
        }
        Object db = ensureMessageDatabase(config);
        if (db == null) {
            return SendResult.failed("WeChat message database is not available for location send verification");
        }
        final ClassLoader targetClassLoader = classLoader;
        final String targetWxid = wxid;
        final double targetLatitude = latitude;
        final double targetLongitude = longitude;
        final int targetScale = Math.max(1, scale);
        final String targetLabel = isBlank(label) ? "[位置]" : label;
        final String targetPoiName = isBlank(poiName) ? targetLabel : poiName;
        final String targetInfoURL = infoURL == null ? "" : infoURL;
        final String targetPoiID = poiID == null ? "" : poiID;
        final boolean targetFromPoiList = fromPoiList;
        final String targetPoiTips = poiTips == null ? "" : poiTips;
        final long beforeMsgId;
        try {
            beforeMsgId = readMaxMessageId(db);
        } catch (Throwable t) {
            return SendResult.failed("location send verification failed before send: " + shortError(t));
        }
        try {
            callOnMainThread(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    sendViaLocationFeature(targetClassLoader, targetWxid, targetLatitude, targetLongitude, targetScale, targetLabel, targetPoiName, targetInfoURL, targetPoiID, targetFromPoiList, targetPoiTips);
                    return null;
                }
            });
        } catch (Throwable t) {
            return SendResult.failed("WeChat location send failed via LocationMsgSendTask: " + shortError(t));
        }

        long msgId = waitForOutgoingMediaMessage(config, targetWxid, beforeMsgId, 10000L, "location", "48");
        if (msgId > 0L) {
            log("sendLocation verified outgoing location msgId=" + msgId);
            return SendResult.sent(msgId);
        }
        return SendResult.failed("WeChat location send invoked but no outgoing location message was recorded");
    }

    private static SendResult sendQuote(BridgeConfig config, ClassLoader classLoader, String wxid, String text, long quoteMsgId, String quoteTalker, String quoteSenderWxid) {
        classLoader = runtimeClassLoader(classLoader);
        if (classLoader == null) {
            return SendResult.failed("WeChat classLoader is not available");
        }
        if (isBlank(wxid) || isBlank(text) || quoteMsgId <= 0L || isBlank(quoteTalker)) {
            return SendResult.failed("wxid, text, quote_msg_id, and quote_talker are required");
        }
        Object db = ensureMessageDatabase(config);
        if (db == null) {
            return SendResult.failed("WeChat message database is not available for quote send verification");
        }
        final ClassLoader targetClassLoader = classLoader;
        final String targetWxid = wxid;
        final String targetText = text;
        final long beforeMsgId;
        final QuoteSource quoteSource;
        try {
            beforeMsgId = readMaxMessageId(db);
            quoteSource = loadQuoteSource(db, quoteMsgId, quoteTalker);
            if (quoteSource != null && !isBlank(quoteSenderWxid)) {
                quoteSource.senderWxid = quoteSenderWxid.trim();
            }
        } catch (Throwable t) {
            return SendResult.failed("quote send verification failed before send: " + shortError(t));
        }
        if (quoteSource == null) {
            return SendResult.failed("quoted message was not found in local WeChat message database");
        }
        try {
            Long createdMsgId = callOnMainThread(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    return Long.valueOf(sendViaQuoteAppMsg(targetClassLoader, targetWxid, targetText, quoteSource));
                }
            });
            if (createdMsgId == null || createdMsgId.longValue() <= 0L) {
                log("sendQuote appmsg sender returned no message id; wait for outgoing quote record");
            }
        } catch (Throwable t) {
            return SendResult.failed("WeChat quote send failed via appmsg quote path: " + shortError(t));
        }

        long msgId = waitForOutgoingMediaMessage(config, targetWxid, beforeMsgId, 8000L, "quote", String.valueOf(MESSAGE_TYPE_QUOTE));
        if (msgId > 0L) {
            try {
                boolean inserted = insertMsgQuoteRecord(targetClassLoader, msgId, quoteSource);
                log("sendQuote MsgQuote record ensured msgId=" + msgId + " inserted=" + inserted);
            } catch (Throwable t) {
                log("sendQuote MsgQuote record ensure failed: " + shortError(t));
            }
            log("sendQuote verified outgoing quote msgId=" + msgId);
            return SendResult.sent(msgId);
        }
        return SendResult.failed("WeChat quote send invoked but no outgoing quote message was recorded");
    }

    private static SendResult sendRevoke(BridgeConfig config, ClassLoader classLoader, String wxid, long chatRecordId) {
        classLoader = runtimeClassLoader(classLoader);
        if (classLoader == null) {
            return SendResult.failed("WeChat classLoader is not available");
        }
        if (isBlank(wxid) || chatRecordId <= 0L) {
            return SendResult.failed("wxid and chat_record_id are required");
        }
        Object db = ensureMessageDatabase(config);
        if (db == null) {
            return SendResult.failed("WeChat message database is not available for revoke");
        }
        final ClassLoader targetClassLoader = classLoader;
        final RevokeSource source;
        try {
            source = loadRevokeSource(db, chatRecordId, wxid);
        } catch (Throwable t) {
            return SendResult.failed("revoke source lookup failed: " + shortError(t));
        }
        if (source == null) {
            return SendResult.failed("source message was not found in local WeChat message database");
        }
        if (source.isSend != 1) {
            return SendResult.failed("only locally sent messages can be revoked");
        }
        if (isBlank(source.talker)) {
            return SendResult.failed("source message talker is empty");
        }
        if (!wxid.equals(source.talker)) {
            return SendResult.failed("revoke target wxid does not match source message talker");
        }
        if (source.msgSvrId <= 0L) {
            return SendResult.failed("source message msgSvrId is missing");
        }
        if (source.createTime <= 0L) {
            return SendResult.failed("source message createTime is missing");
        }
        try {
            Boolean published = callOnMainThread(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    return Boolean.valueOf(revokeViaRevokeMsgEvent(targetClassLoader, source));
                }
            });
            if (!Boolean.TRUE.equals(published)) {
                return SendResult.failed("WeChat revoke event had no listener");
            }
            log("sendRevoke invoked talker=" + source.talker + " msgId=" + source.msgId + " msgSvrId=" + source.msgSvrId);
            return SendResult.sent(source.msgId > 0L ? source.msgId : chatRecordId);
        } catch (Throwable t) {
            return SendResult.failed("WeChat revoke failed via RevokeMsgEvent: " + detailedErrorChain(t));
        }
    }

    private static SendResult sendChatHistory(BridgeConfig config, ClassLoader classLoader, String wxid, String title, String description, String recordItemXML) {
        classLoader = runtimeClassLoader(classLoader);
        if (classLoader == null) {
            return SendResult.failed("WeChat classLoader is not available");
        }
        if (isBlank(wxid) || isBlank(title) || isBlank(recordItemXML)) {
            return SendResult.failed("wxid, title, and recorditem_xml are required");
        }
        Object db = ensureMessageDatabase(config);
        if (db == null) {
            return SendResult.failed("WeChat message database is not available for chat_history send verification");
        }
        final ClassLoader targetClassLoader = classLoader;
        final String targetWxid = wxid;
        final String targetTitle = title;
        final String targetDescription = description;
        final String targetRecordItemXML = recordItemXML;
        final long beforeMsgId;
        try {
            beforeMsgId = readMaxMessageId(db);
        } catch (Throwable t) {
            return SendResult.failed("chat_history send verification failed before send: " + shortError(t));
        }
        try {
            Long createdMsgId = callOnMainThread(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    return Long.valueOf(sendViaChatHistoryAppMsg(targetClassLoader, targetWxid, targetTitle, targetDescription, targetRecordItemXML));
                }
            });
            if (createdMsgId == null || createdMsgId.longValue() <= 0L) {
                log("sendChatHistory appmsg sender returned no message id; wait for outgoing chat_history record");
            }
        } catch (Throwable t) {
            return SendResult.failed("WeChat chat_history send failed via appmsg path: " + shortError(t));
        }

        long msgId = waitForOutgoingAppMsgMessage(config, targetWxid, beforeMsgId, 10000L, "chat_history", APPMSG_TYPE_CHAT_HISTORY);
        if (msgId > 0L) {
            postVerifiedOutgoingMessage(config, db, msgId, "chat_history");
            log("sendChatHistory verified outgoing chat_history msgId=" + msgId);
            return SendResult.sent(msgId);
        }
        return SendResult.failed("WeChat chat_history send invoked but no outgoing chat_history message was recorded");
    }

    private static SendResult sendLink(BridgeConfig config, ClassLoader classLoader, String wxid, String title, String description, String url, String appName, String thumbUrl) {
        classLoader = runtimeClassLoader(classLoader);
        if (classLoader == null) {
            return SendResult.failed("WeChat classLoader is not available");
        }
        if (isBlank(wxid) || isBlank(title) || isBlank(url)) {
            return SendResult.failed("wxid, appmsg_title, and appmsg_url are required");
        }
        try {
            Class<?> appMsgClass = findVersionedClass(classLoader, "ot0.q");
            Object appMsg = appMsgClass.getDeclaredConstructor().newInstance();
            setObjectField(appMsg, title, "f");
            setObjectField(appMsg, description == null ? "" : description, "g");
            setIntFieldAny(appMsg, APPMSG_TYPE_LINK, "i");
            setObjectField(appMsg, url, "k");
            setObjectField(appMsg, url, "l");
            if (!isBlank(appName)) {
                setObjectField(appMsg, appName, "x");
            }
            if (!isBlank(thumbUrl)) {
                setObjectField(appMsg, thumbUrl, "z");
            }
            return sendAppMsgObject(config, classLoader, wxid, appMsg, "link", APPMSG_TYPE_LINK);
        } catch (Throwable t) {
            return SendResult.failed("WeChat link send failed via appmsg path: " + shortError(t));
        }
    }

    private static SendResult sendMiniProgram(BridgeConfig config, ClassLoader classLoader, String wxid, String title, String description, String url, String appName, String username, String pagePath, String appId, String iconUrl, int version, int miniType) {
        classLoader = runtimeClassLoader(classLoader);
        if (classLoader == null) {
            return SendResult.failed("WeChat classLoader is not available");
        }
        if (isBlank(wxid) || isBlank(title) || isBlank(username) || isBlank(pagePath)) {
            return SendResult.failed("wxid, appmsg_title, mini_program_username, and mini_program_page_path are required");
        }
        try {
            WeChatVersionSymbols.Profile symbols = weChatSymbols(classLoader);
            Class<?> appMsgClass = findVersionedClass(classLoader, "ot0.q");
            Object appMsg = appMsgClass.getDeclaredConstructor().newInstance();
            setObjectField(appMsg, title, "f");
            setObjectField(appMsg, description == null ? "" : description, "g");
            setIntFieldAny(appMsg, APPMSG_TYPE_MINI_PROGRAM, "i");
            setObjectField(appMsg, url == null ? "" : url, "k");
            setObjectField(appMsg, username, symbols.fields.miniProgramUsername);
            setObjectField(appMsg, pagePath, symbols.fields.miniProgramPagePath);
            setObjectField(appMsg, appId == null ? "" : appId, symbols.fields.miniProgramAppId);
            setObjectField(appMsg, iconUrl == null ? "" : iconUrl, symbols.fields.miniProgramIconUrl);
            if (!isBlank(appName)) {
                setObjectField(appMsg, appName, "x");
                setObjectField(appMsg, appName, symbols.fields.miniProgramSourceName);
            }
            if (version > 0) {
                setIntFieldAny(appMsg, version, symbols.fields.miniProgramVersion);
            }
            if (miniType > 0) {
                setIntFieldAny(appMsg, miniType, symbols.fields.miniProgramType);
            }
            return sendAppMsgObject(config, classLoader, wxid, appMsg, "mini_program", APPMSG_TYPE_MINI_PROGRAM, APPMSG_TYPE_MINI_PROGRAM_LEGACY);
        } catch (Throwable t) {
            return SendResult.failed("WeChat mini_program send failed via appmsg path: " + shortError(t));
        }
    }

    private static SendResult sendSourceAppMsg(BridgeConfig config, ClassLoader classLoader, String wxid, long sourceChatRecordId, String kind, int... expectedAppMsgTypes) {
        classLoader = runtimeClassLoader(classLoader);
        if (classLoader == null) {
            return SendResult.failed("WeChat classLoader is not available");
        }
        Object db = ensureMessageDatabase(config);
        if (db == null) {
            return SendResult.failed("WeChat message database is not available for " + kind + " source send");
        }
        try {
            ChatHistorySource source = loadChatHistorySource(db, sourceChatRecordId);
            if (source == null) {
                return SendResult.failed("source appmsg was not found in local WeChat message database");
            }
            String sourceContent = normalizeMessageText(source.talker, source.content);
            if (source.type != MESSAGE_TYPE_APPMSG) {
                return SendResult.failed("source_chat_record_id must point to an appmsg");
            }
            int appMsgType = appMsgTypeFromContent(sourceContent);
            if (!containsInt(expectedAppMsgTypes, appMsgType)) {
                return SendResult.failed("source appmsg type does not match " + kind);
            }
            Object appMsg = parseAppMsgObject(classLoader, sourceContent);
            if (appMsg == null) {
                return SendResult.failed("source appmsg XML could not be parsed by WeChat");
            }
            return sendAppMsgObject(config, classLoader, wxid, appMsg, kind, expectedAppMsgTypes);
        } catch (Throwable t) {
            return SendResult.failed(kind + " source send failed: " + shortError(t));
        }
    }

    private static Object parseAppMsgObject(ClassLoader classLoader, String sourceContent) throws Exception {
        if (isBlank(sourceContent)) {
            return null;
        }
        Class<?> appMsgClass = findVersionedClass(classLoader, "ot0.q");
        Object parsed = findMethod(appMsgClass, "v", String.class).invoke(null, sourceContent);
        return appMsgClass.isInstance(parsed) ? parsed : null;
    }

    private static SendResult sendAppMsgObject(BridgeConfig config, ClassLoader classLoader, String wxid, Object appMsg, String kind, int... expectedAppMsgTypes) {
        if (isBlank(wxid) || appMsg == null || expectedAppMsgTypes == null || expectedAppMsgTypes.length == 0) {
            return SendResult.failed("wxid, appmsg, and expected appmsg type are required");
        }
        Object db = ensureMessageDatabase(config);
        if (db == null) {
            return SendResult.failed("WeChat message database is not available for " + kind + " send verification");
        }
        final ClassLoader targetClassLoader = classLoader;
        final String targetWxid = wxid;
        final Object targetAppMsg = appMsg;
        final int[] targetTypes = expectedAppMsgTypes;
        final long beforeMsgId;
        try {
            beforeMsgId = readMaxMessageId(db);
        } catch (Throwable t) {
            return SendResult.failed(kind + " send verification failed before send: " + shortError(t));
        }
        try {
            Long createdMsgId = callOnMainThread(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    return Long.valueOf(sendViaAppMsgObject(targetClassLoader, targetWxid, targetAppMsg));
                }
            });
            if (createdMsgId == null || createdMsgId.longValue() <= 0L) {
                log("send " + kind + " appmsg sender returned no message id; wait for outgoing appmsg record");
            }
        } catch (Throwable t) {
            return SendResult.failed("WeChat " + kind + " send failed via appmsg path: " + shortError(t));
        }
        long msgId = waitForOutgoingAppMsgMessage(config, targetWxid, beforeMsgId, 10000L, kind, targetTypes);
        if (msgId > 0L) {
            postVerifiedOutgoingMessage(config, db, msgId, kind);
            log("send " + kind + " verified outgoing appmsg msgId=" + msgId);
            return SendResult.sent(msgId);
        }
        return SendResult.failed("WeChat " + kind + " send invoked but no outgoing appmsg message was recorded");
    }

    private static long sendViaAppMsgObject(ClassLoader classLoader, String wxid, Object appMsg) throws Exception {
        Class<?> appMsgClass = findVersionedClass(classLoader, "ot0.q");
        Class<?> appMsgLogicClass = findClass(classLoader, "com.tencent.mm.pluginsdk.model.app.k0");
        Method send = findMethod(appMsgLogicClass, "I", appMsgClass,
                String.class, String.class, String.class, String.class, byte[].class);
        Object pair = send.invoke(null, appMsg, "", "", wxid, "", null);
        return pair == null ? 0L : longPairField(pair, "second");
    }

    private static QuoteSource loadQuoteSource(Object db, long quoteMsgId, String quoteTalker) throws Exception {
        QuoteSource source = queryQuoteSource(db, quoteMsgId, quoteTalker, true, true);
        if (source != null) {
            return source;
        }
        source = queryQuoteSource(db, quoteMsgId, quoteTalker, true, false);
        if (source != null) {
            return source;
        }
        source = queryQuoteSource(db, quoteMsgId, quoteTalker, false, true);
        if (source != null) {
            return source;
        }
        return queryQuoteSource(db, quoteMsgId, quoteTalker, false, false);
    }

    private static RevokeSource loadRevokeSource(Object db, long msgId, String talker) throws Exception {
        RevokeSource source = queryRevokeSource(db, msgId, talker, true);
        if (source != null) {
            return source;
        }
        return queryRevokeSource(db, msgId, talker, false);
    }

    private static RevokeSource queryRevokeSource(Object db, long msgId, String talker, boolean requireTalker) throws Exception {
        String where = requireTalker ? "msgId = ? AND talker = ?" : "msgId = ?";
        String[] args = requireTalker
                ? new String[]{String.valueOf(msgId), talker}
                : new String[]{String.valueOf(msgId)};
        Object cursor = rawQuery(db, ""
                + "SELECT msgId,COALESCE(msgSvrId,0),talker,isSend,createTime,type "
                + "FROM message "
                + "WHERE " + where + " "
                + "LIMIT 1", args);
        if (cursor == null) {
            return null;
        }
        try {
            Method moveToFirst = findNoArgMethod(cursor.getClass(), "moveToFirst");
            if (!Boolean.TRUE.equals(moveToFirst.invoke(cursor))) {
                return null;
            }
            RevokeSource source = new RevokeSource();
            int column = 0;
            source.msgId = longColumn(cursor, column++);
            source.msgSvrId = longColumn(cursor, column++);
            source.talker = stringColumn(cursor, column++);
            source.isSend = intColumn(cursor, column++);
            source.createTime = normalizeCreateTime(longColumn(cursor, column++));
            source.type = intColumn(cursor, column);
            return source.msgId > 0L ? source : null;
        } finally {
            closeQuietly(cursor);
        }
    }

    private static QuoteSource queryQuoteSource(Object db, long quoteMsgId, String quoteTalker, boolean requireTalker, boolean includeMsgSource) throws Exception {
        String msgSourceSelect = includeMsgSource ? ",COALESCE(msgSource,'')" : ",''";
        String where = requireTalker ? "msgId = ? AND talker = ?" : "msgId = ?";
        String[] args = requireTalker
                ? new String[]{String.valueOf(quoteMsgId), quoteTalker}
                : new String[]{String.valueOf(quoteMsgId)};
        Object cursor;
        try {
            cursor = rawQuery(db, ""
                    + "SELECT msgId,COALESCE(msgSvrId,0),talker,COALESCE(content,''),isSend,createTime,type"
                    + msgSourceSelect + " "
                    + "FROM message "
                    + "WHERE " + where + " "
                    + "LIMIT 1", args);
        } catch (Throwable t) {
            if (includeMsgSource) {
                return null;
            }
            throw t;
        }
        if (cursor == null) {
            return null;
        }
        try {
            Method moveToFirst = findNoArgMethod(cursor.getClass(), "moveToFirst");
            if (!Boolean.TRUE.equals(moveToFirst.invoke(cursor))) {
                return null;
            }
            QuoteSource source = new QuoteSource();
            int column = 0;
            source.msgId = longColumn(cursor, column++);
            source.msgSvrId = longColumn(cursor, column++);
            source.talker = stringColumn(cursor, column++);
            source.content = stringColumn(cursor, column++);
            source.isSend = intColumn(cursor, column++);
            source.createTime = normalizeCreateTime(longColumn(cursor, column++));
            source.type = intColumn(cursor, column++);
            source.msgSource = stringColumn(cursor, column);
            return source.msgId > 0L ? source : null;
        } finally {
            closeQuietly(cursor);
        }
    }

    private static boolean revokeViaRevokeMsgEvent(ClassLoader classLoader, RevokeSource source) throws Exception {
        boolean preferImage = isImageLikeMessageType(source == null ? 0 : source.type);
        Object messageRecord = null;
        try {
            messageRecord = lookupMessageRecord(classLoader, source);
        } catch (Throwable t) {
            log("revoke message record lookup failed, payload's message-record fields will stay null: " + shortError(t));
        }
        if (publishRevokeMsgEvent(classLoader, source, preferImage, messageRecord)) {
            return true;
        }
        return publishRevokeMsgEvent(classLoader, source, !preferImage, messageRecord);
    }

    /**
     * Per handoff doc: RevokeMsgEvent's payload class (baseline "fm.fs") holds
     * two com.tencent.mm.storage.f9 (message row) fields that applyRevokePayload
     * never filled in -- WeChat's own revoke handler dereferences one of these
     * to look up the message's content/wording and crashes with a
     * NullPointerException when it is null. This is not new in 8.0.78; the
     * bug just never showed up before because revoke was never exercised this
     * thoroughly. Reuse the existing message-lookup entry point (baseline
     * "qt0.k0" / imageMessageLookup) that the image-path code already relies
     * on to fetch a fully-populated message row object for these fields.
     */
    private static Object lookupMessageRecord(ClassLoader classLoader, RevokeSource source) throws Exception {
        if (source == null || isBlank(source.talker) || source.msgId <= 0L) {
            return null;
        }
        WeChatVersionSymbols.Profile symbols = weChatSymbols(classLoader);
        Class<?> messageStorageClass = findVersionedClass(classLoader, "qt0.k0");
        return findMethod(messageStorageClass, symbols.methods.imageMessageLookup, String.class, long.class)
                .invoke(null, source.talker, source.msgId);
    }

    private static boolean publishRevokeMsgEvent(ClassLoader classLoader, RevokeSource source, boolean isImage, Object messageRecord) throws Exception {
        try {
            Class<?> eventClass = findClass(classLoader, "com.tencent.mm.autogen.events.RevokeMsgEvent");
            if (eventClass == null) {
                throw new ClassNotFoundException("RevokeMsgEvent class not found in current WeChat version");
            }
            Object event = eventClass.getDeclaredConstructor().newInstance();
            Object payload = resolveEventPayloadObject(event);
            if (payload == null) {
                throw new IllegalStateException("RevokeMsgEvent payload is unavailable");
            }
            log("publishRevokeMsgEvent payloadClass=" + payload.getClass().getName() + " isImage=" + isImage
                    + " messageRecord=" + (messageRecord == null ? "null" : messageRecord.getClass().getName()));
            applyRevokePayload(payload, source, isImage, messageRecord);
            Method eMethod = findNoArgMethod(event.getClass(), "e");
            if (eMethod == null) {
                throw new NoSuchMethodException("RevokeMsgEvent.e() not found");
            }
            log("publishRevokeMsgEvent invoking e() on " + event.getClass().getName());
            Object result = eMethod.invoke(event);
            boolean success = !(result instanceof Boolean) || ((Boolean) result).booleanValue();
            log("publishRevokeMsgEvent result=" + result + " success=" + success);
            return success;
        } catch (InvocationTargetException ite) {
            Throwable cause = ite.getCause() != null ? ite.getCause() : ite;
            log("publishRevokeMsgEvent InvocationTargetException cause: " + detailedErrorChain(cause));
            throw new Exception("RevokeMsgEvent.e() failed: " + detailedErrorChain(cause), cause);
        }
    }

    private static long sendViaQuoteAppMsg(ClassLoader classLoader, String wxid, String text, QuoteSource source) throws Exception {
        WeChatVersionSymbols.Profile symbols = weChatSymbols(classLoader);
        Class<?> appMsgClass = findVersionedClass(classLoader, "ot0.q");
        Object appMsg = appMsgClass.getDeclaredConstructor().newInstance();
        setObjectField(appMsg, text, "f");
        setIntFieldAny(appMsg, APPMSG_TYPE_QUOTE, "i");

        Object quoteItem = createMsgQuoteItem(classLoader, source);
        setObjectField(appMsg, quoteItem, symbols.fields.appMsgQuoteItem);

        Class<?> appMsgLogicClass = findClass(classLoader, "com.tencent.mm.pluginsdk.model.app.k0");
        Method send = findMethod(appMsgLogicClass, "I", appMsgClass,
                String.class, String.class, String.class, String.class, byte[].class);
        Object pair = send.invoke(null, appMsg, "", "", wxid, "", null);
        long newMsgId = pair == null ? 0L : longPairField(pair, "second");
        if (newMsgId <= 0L) {
            return 0L;
        }
        try {
            boolean inserted = insertMsgQuoteRecord(classLoader, newMsgId, source);
            log("sendQuote appmsg created msgId=" + newMsgId + " quoteRecordInserted=" + inserted);
        } catch (Throwable t) {
            log("sendQuote appmsg created but MsgQuote insert failed: " + shortError(t));
        }
        return newMsgId;
    }

    private static long sendViaChatHistoryAppMsg(ClassLoader classLoader, String wxid, String title, String description, String recordItemXML) throws Exception {
        WeChatVersionSymbols.Profile symbols = weChatSymbols(classLoader);
        Class<?> appMsgClass = findVersionedClass(classLoader, "ot0.q");
        Object appMsg = appMsgClass.getDeclaredConstructor().newInstance();
        setObjectField(appMsg, title, "f");
        setObjectField(appMsg, description == null ? "" : description, "g");
        setIntFieldAny(appMsg, APPMSG_TYPE_CHAT_HISTORY, "i");
        setObjectField(appMsg, recordItemXML, symbols.fields.appMsgRecordItem);

        Class<?> appMsgLogicClass = findClass(classLoader, "com.tencent.mm.pluginsdk.model.app.k0");
        Method send = findMethod(appMsgLogicClass, "I", appMsgClass,
                String.class, String.class, String.class, String.class, byte[].class);
        Object pair = send.invoke(null, appMsg, "", "", wxid, "", null);
        return pair == null ? 0L : longPairField(pair, "second");
    }

    private static Object createMsgQuoteItem(ClassLoader classLoader, QuoteSource source) throws Exception {
        Object quoteItem = newMsgQuoteItem(classLoader);
        String sender = quoteSender(source);
        String quoteContent = normalizeMessageText(source.talker, source.content);
        String msgSource = source.msgSource == null ? "" : source.msgSource;

        setIntFieldAny(quoteItem, quoteAppMsgSourceType(classLoader, source.type), "d");
        setLongFieldAny(quoteItem, firstPositiveLong(source.msgSvrId, source.msgId), "e");
        setObjectField(quoteItem, firstNonBlank(source.talker, ""), "f");
        setObjectField(quoteItem, sender, "g");
        setObjectField(quoteItem, sender, "h");
        setObjectField(quoteItem, msgSource, "i");
        setObjectField(quoteItem, quoteContent, "m");
        setObjectField(quoteItem, msgSource, "n");
        setObjectField(quoteItem, quoteStrID(classLoader, msgSource), "p");
        setLongFieldAny(quoteItem, source.createTime, "q");
        return quoteItem;
    }

    private static Object newMsgQuoteItem(ClassLoader classLoader) throws Exception {
        Class<?> quoteItemClass = findClass(classLoader, "com.tencent.mm.plugin.msgquote.model.MsgQuoteItem");
        try {
            Constructor<?> ctor = quoteItemClass.getDeclaredConstructor();
            ctor.setAccessible(true);
            return ctor.newInstance();
        } catch (NoSuchMethodException ignored) {
            Parcel parcel = Parcel.obtain();
            try {
                parcel.writeInt(0);
                parcel.writeLong(0L);
                parcel.writeString("");
                parcel.writeString("");
                parcel.writeString("");
                parcel.writeString("");
                parcel.writeString("");
                parcel.writeString("");
                parcel.writeInt(0);
                parcel.writeString("");
                parcel.writeLong(0L);
                parcel.writeString("");
                parcel.setDataPosition(0);
                Constructor<?> ctor = quoteItemClass.getDeclaredConstructor(Parcel.class);
                ctor.setAccessible(true);
                return ctor.newInstance(parcel);
            } finally {
                parcel.recycle();
            }
        }
    }

    private static int quoteAppMsgSourceType(ClassLoader classLoader, int messageType) {
        try {
            Class<?> appMsgLogicClass = findClass(classLoader, "com.tencent.mm.pluginsdk.model.app.k0");
            Object value = findMethod(appMsgLogicClass, "c", int.class).invoke(null, messageType);
            if (value instanceof Number) {
                int mapped = ((Number) value).intValue();
                return mapped > 0 ? mapped : messageType;
            }
        } catch (Throwable t) {
            log("quote source type mapping skipped: " + shortError(t));
        }
        return messageType;
    }

    private static String quoteSender(QuoteSource source) {
        if (source == null) {
            return "";
        }
        String sender = isChatroomTalker(source.talker) ? firstNonBlank(source.senderWxid, extractChatroomSender(source.content)) : "";
        return firstNonBlank(sender, source.talker);
    }

    private static String quoteStrID(ClassLoader classLoader, String msgSource) {
        if (isBlank(msgSource)) {
            return "";
        }
        try {
            Object value = findMethod(findVersionedClass(classLoader, "c01.ia"), "t", String.class).invoke(null, msgSource);
            return value == null ? "" : String.valueOf(value);
        } catch (Throwable ignored) {
            return "";
        }
    }

    private static boolean insertMsgQuoteRecord(ClassLoader classLoader, long newMsgId, QuoteSource source) throws Exception {
        WeChatVersionSymbols.Profile symbols = weChatSymbols(classLoader);
        Class<?> pluginInterface = findVersionedClass(classLoader, "aa0.e");
        Object plugin = findMethod(findVersionedClass(classLoader, "i95.n0"), "c", Class.class).invoke(null, pluginInterface);
        if (plugin == null) {
            throw new IllegalStateException("msgquote plugin service is unavailable");
        }
        Object storage;
        try {
            storage = findNoArgMethod(plugin.getClass(), symbols.methods.quoteStorage).invoke(plugin);
        } catch (NoSuchMethodException e) {
            // WeChat 8.0.78 restructured this plugin from "instance method
            // returns the storage object" into a static-factory shape (the
            // plugin class itself exposes static getters for its fields
            // instead). Try the static getter that structurally matches the
            // old storage object's role (same superclass lineage as the
            // pre-8.0.78 "sn3.a" storage type).
            Method staticGetter = findMethod(plugin.getClass(), "cj");
            if (!Modifier.isStatic(staticGetter.getModifiers())) {
                throw e;
            }
            storage = staticGetter.invoke(null);
        }
        if (storage == null) {
            throw new IllegalStateException("MsgQuote storage is unavailable");
        }

        // "ui3.b" (baseline; 8.0.77 real name "sn3.b") is a bare, empty
        // subclass of "im.e8" that adds nothing of its own beyond clone()/
        // toString() -- every field this code actually sets (field_msgId
        // etc.) lives on "im.e8" itself, and that class's own name and field
        // layout is unchanged across 8.0.77/8.0.78. Construct "im.e8"
        // directly instead of chasing the unstable short-named subclass.
        Class<?> recordClass = findClass(classLoader, "im.e8");
        Object record = recordClass.getDeclaredConstructor().newInstance();
        setLongFieldAny(record, newMsgId, "field_msgId");
        setLongFieldAny(record, source.msgId, "field_quotedMsgId");
        setLongFieldAny(record, source.msgSvrId, "field_quotedMsgSvrId");
        setObjectField(record, source.talker, "field_quotedMsgTalker");
        Method insert = symbols.featureServiceSendStack
                ? findMethod(storage.getClass(), "insert", recordClass)
                : findMethod(storage.getClass(), "J0", recordClass);
        Object result = insert.invoke(storage, record);
        return Boolean.TRUE.equals(result);
    }

    private static long longPairField(Object pair, String fieldName) throws Exception {
        Field field = findFieldAny(pair.getClass(), fieldName);
        Object value = field.get(pair);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return value == null ? 0L : Long.parseLong(String.valueOf(value));
    }

    private static void sendViaSendImgMgr(ClassLoader classLoader, String wxid, File imageFile) throws Exception {
        Context context = APP_CONTEXT;
        if (context == null) {
            Object application = currentApplication();
            if (application instanceof Context) {
                context = (Context) application;
            }
        }
        if (context == null) {
            throw new IllegalStateException("WeChat context is not available");
        }
        WeChatVersionSymbols.Profile symbols = weChatSymbols(classLoader);
        Object service;
        if (symbols.featureServiceSendStack) {
            Class<?> serviceInterface = findVersionedClass(classLoader, "qh3.a1");
            service = findMethod(findVersionedClass(classLoader, "i95.n0"), "c", Class.class)
                    .invoke(null, serviceInterface);
        } else {
            Class<?> accessorClass = findClass(classLoader, "tg3.t1");
            Method accessor = findNoArgMethod(accessorClass, "a");
            service = accessor.invoke(null);
        }
        if (service == null) {
            throw new IllegalStateException("image send service is not available");
        }
        Class<?> forwardInfoClass = findVersionedClass(classLoader, "c01.h7");
        if (symbols.featureServiceSendStack) {
            try {
                Method send = findMethod(service.getClass(), symbols.methods.imageShortSend,
                        Context.class, String.class, String.class, int.class,
                        String.class, String.class, forwardInfoClass);
                send.invoke(service, context, wxid, imageFile.getAbsolutePath(), 1, "", "", null);
                return;
            } catch (NoSuchMethodException unavailable) {
                Class<?> scanCodeInfoClass = findClass(classLoader, "com.tencent.mm.modelscan.ScanCodeInfo");
                Class<?> sourceImgInfoClass = findClass(classLoader, "com.tencent.mm.modelmulti.SourceImgInfo");
                Method send = findMethod(service.getClass(), symbols.methods.imageLongSend,
                        Context.class, String.class, String.class, int.class,
                        String.class, String.class, forwardInfoClass,
                        scanCodeInfoClass, sourceImgInfoClass);
                send.invoke(service, context, wxid, imageFile.getAbsolutePath(), 1, "", "", null, null, null);
                return;
            }
        }
        try {
            Method send = findMethod(service.getClass(), "Vi",
                    Context.class, String.class, String.class, int.class, String.class, String.class, forwardInfoClass);
            send.invoke(service, context, wxid, imageFile.getAbsolutePath(), 1, "", "", null);
            return;
        } catch (NoSuchMethodException unavailable) {
            Class<?> scanCodeInfoClass = findClass(classLoader, "com.tencent.mm.modelscan.ScanCodeInfo");
            Class<?> sourceImgInfoClass = findClass(classLoader, "com.tencent.mm.modelmulti.SourceImgInfo");
            Method send = findMethod(service.getClass(), "aj",
                    Context.class, String.class, String.class, int.class, String.class, String.class,
                    forwardInfoClass, scanCodeInfoClass, sourceImgInfoClass);
            send.invoke(service, context, wxid, imageFile.getAbsolutePath(), 1, "", "", null, null, null);
        }
    }

    private static void sendViaVideoSendFeature(ClassLoader classLoader, String wxid, File videoFile) throws Exception {
        WeChatVersionSymbols.Profile symbols = weChatSymbols(classLoader);
        Class<?> r2Class = findVersionedClass(classLoader, "vf0.r2");
        Object crossParams = defaultVideoCrossParams(classLoader, r2Class);

        Class<?> forwardInfoClass = findVersionedClass(classLoader, "c01.h7");
        Class<?> u2Class = findVersionedClass(classLoader, "vf0.u2");
        Constructor<?> elementCtor = findConstructor(
                u2Class,
                String.class,
                String.class,
                String.class,
                boolean.class,
                int.class,
                r2Class,
                forwardInfoClass);
        Object element = elementCtor.newInstance(
                videoFileNamePure(videoFile),
                videoFile.getAbsolutePath(),
                "",
                false,
                0,
                crossParams,
                null);

        Class<?> i3Class = findVersionedClass(classLoader, "vf0.i3");
        Class<?> h3Class = findVersionedClass(classLoader, "vf0.h3");
        Constructor<?> paramsCtor = findConstructor(h3Class, String.class, u2Class, boolean.class, i3Class);
        Object params = paramsCtor.newInstance(wxid, element, false, null);

        Class<?> serviceInterface = findVersionedClass(classLoader, "wf0.b2");
        Method getService = findMethod(findVersionedClass(classLoader, "i95.n0"), "c", Class.class);
        Object videoService = getService.invoke(null, serviceInterface);
        if (videoService == null) {
            throw new IllegalStateException("wf0.b2 service is not available");
        }
        Method buildTask = findMethod(
                videoService.getClass(),
                symbols.methods.videoBuildTask,
                h3Class);
        Object task = buildTask.invoke(videoService, params);
        if (task == null) {
            throw new IllegalStateException("video task build returned null");
        }

        Class<?> taskInterface = findVersionedClass(classLoader, "qi3.b0");
        Class<?> dispatcherClass = findVersionedClass(classLoader, "qi3.x");
        Object dispatcher = findField(dispatcherClass, "a").get(null);
        if (dispatcher == null) {
            throw new IllegalStateException("video task dispatcher is not available");
        }
        Method sendAsync = findMethod(dispatcherClass, "d", taskInterface);
        sendAsync.invoke(dispatcher, task);
        log("sendVideo invoked VideoSendFeature task=" + task.getClass().getName() + " size=" + videoFile.length());
    }

    private static void sendViaVoiceSendTask(ClassLoader classLoader, String wxid, File voiceFile, int durationMs) throws Exception {
        WeChatVersionSymbols.Profile symbols = weChatSymbols(classLoader);
        String voiceFileName = voiceFileNamePure(voiceFile);
        String preparedPath = prepareWeChatVoiceFile(classLoader, voiceFileName, voiceFile);

        Class<?> paramsClass = findVersionedClass(classLoader, "cg0.d");
        Constructor<?> paramsCtor = findConstructor(paramsClass, String.class, String.class);
        Object params = paramsCtor.newInstance(wxid, voiceFileName);
        setOptionalIntField(params, Math.max(1000, durationMs), "h");
        setOptionalIntField(params, 0, "i");

        Class<?> taskClass = findVersionedClass(classLoader, "jg0.x");
        Constructor<?> taskCtor = findConstructor(taskClass, paramsClass);
        Object task = taskCtor.newInstance(params);

        Class<?> serviceInterface = findVersionedClass(classLoader, "dg0.f");
        Object service = findMethod(findVersionedClass(classLoader, "i95.n0"), "c", Class.class).invoke(null, serviceInterface);
        if (service == null) {
            throw new IllegalStateException("VoiceMsgFeatureService is not available");
        }
        Class<?> taskInterface = findVersionedClass(classLoader, "qi3.b0");
        Method send = findMethod(
                service.getClass(),
                symbols.methods.voiceSendTask,
                taskInterface);
        send.invoke(service, task);
        log("sendVoice invoked VoiceMsgSendTask fileName=" + voiceFileName + " path=" + preparedPath + " size=" + voiceFile.length());
    }

    private static void sendViaFileSendLogic(ClassLoader classLoader, String wxid, File file) throws Exception {
        Class<?> paramsClass = findVersionedClass(classLoader, "ut.s0");
        Constructor<?> paramsCtor = findConstructor(paramsClass, String.class, String.class);
        Object params = paramsCtor.newInstance(wxid, file.getAbsolutePath());

        Class<?> fileSendLogicHolder = findVersionedClass(classLoader, "dk5.w");
        Object logic = findField(fileSendLogicHolder, "a").get(null);
        if (logic == null) {
            throw new IllegalStateException("FileSendLogic is not available");
        }
        Method send = findMethod(logic.getClass(), "c", paramsClass);
        send.invoke(logic, params);
        log("sendFile invoked FileSendLogic size=" + file.length());
    }

    private static void sendViaEmojiMgr(ClassLoader classLoader, String wxid, String emojiMd5) throws Exception {
        WeChatVersionSymbols.Profile symbols = weChatSymbols(classLoader);
        Object emojiInfo = loadEmojiInfoByMd5(classLoader, emojiMd5);
        if (emojiInfo == null) {
            throw new IllegalStateException("emoji info was not found in local WeChat storage");
        }
        Class<?> featureClass = findClass(classLoader, "com.tencent.mm.feature.emoji.b0");
        Object feature = findMethod(findVersionedClass(classLoader, "i95.n0"), "c", Class.class).invoke(null, featureClass);
        if (feature == null) {
            throw new IllegalStateException("EmojiFeatureService is not available");
        }
        Object emojiMgr = findNoArgMethod(
                feature.getClass(),
                symbols.methods.emojiManager).invoke(feature);
        if (emojiMgr == null) {
            throw new IllegalStateException("EmojiMgrImpl is not available");
        }
        Class<?> emojiInfoClass = findClass(classLoader, "com.tencent.mm.storage.emotion.EmojiInfo");
        Class<?> messageClass = findVersionedClass(classLoader, "com.tencent.mm.storage.f9");
        Class<?> callbackClass = findVersionedClass(classLoader, "r15.b");
        Method send = findMethod(emojiMgr.getClass(), "Y", String.class, emojiInfoClass, messageClass, callbackClass, int.class);
        send.invoke(emojiMgr, wxid, emojiInfoClass.cast(emojiInfo), null, null, emojiOrdinal(emojiInfo));
        log("sendEmoji invoked EmojiMgrImpl");
    }

    private static void logEmojiInfoDiagnostic(String emojiMd5) {
        String md5 = normalizeEmojiMd5(emojiMd5);
        if (!looksLikeMd5Hex(md5)) {
            return;
        }
        synchronized (EMOJI_INFO_DIAGNOSTIC_REPORTED_MD5) {
            if (!EMOJI_INFO_DIAGNOSTIC_REPORTED_MD5.add(md5)) {
                return;
            }
            if (EMOJI_INFO_DIAGNOSTIC_REPORTED_MD5.size() > 80) {
                EMOJI_INFO_DIAGNOSTIC_REPORTED_MD5.clear();
                EMOJI_INFO_DIAGNOSTIC_REPORTED_MD5.add(md5);
            }
        }
        try {
            ClassLoader classLoader = WECHAT_CLASS_LOADER;
            classLoader = runtimeClassLoader(classLoader == null ? HookEntry.class.getClassLoader() : classLoader);
            Object emojiInfo = loadEmojiInfoByMd5(classLoader, md5);
            if (emojiInfo == null) {
                log("emoji info missing md5=" + shortMd5(md5));
                return;
            }
            log("emoji info found md5=" + shortMd5(md5)
                    + " class=" + emojiInfo.getClass().getName()
                    + " fields=" + emojiInfoFieldSummary(emojiInfo));
        } catch (Throwable t) {
            log("emoji info diagnostic failed md5=" + shortMd5(md5) + " error=" + shortError(t));
        }
    }

    private static String emojiInfoFieldSummary(Object emojiInfo) {
        if (emojiInfo == null) {
            return "[]";
        }
        StringBuilder out = new StringBuilder("[");
        Set<String> seen = new HashSet<>();
        int count = 0;
        Class<?> current = emojiInfo.getClass();
        while (current != null && count < 12) {
            Field[] fields = current.getDeclaredFields();
            for (Field field : fields) {
                if (count >= 12) {
                    break;
                }
                if (field == null
                        || field.getType() != String.class
                        || Modifier.isStatic(field.getModifiers())
                        || !seen.add(field.getName())) {
                    continue;
                }
                try {
                    field.setAccessible(true);
                    Object value = field.get(emojiInfo);
                    if (value == null || isBlank(String.valueOf(value))) {
                        continue;
                    }
                    if (count > 0) {
                        out.append(", ");
                    }
                    out.append(field.getName()).append("=").append(summarizeEmojiInfoValue(String.valueOf(value)));
                    count++;
                } catch (Throwable ignored) {
                    // Diagnostics only; skip fields that are not readable on this build.
                }
            }
            current = current.getSuperclass();
        }
        if (count == 0) {
            out.append("no-string-fields");
        }
        out.append("]");
        return out.toString();
    }

    private static String summarizeEmojiInfoValue(String value) {
        if (isBlank(value)) {
            return "";
        }
        String trimmed = value.trim();
        String lower = trimmed.toLowerCase(Locale.US);
        if (lower.startsWith("http://") || lower.startsWith("https://")) {
            return "<url>";
        }
        if (looksLikeMd5Hex(trimmed)) {
            return shortMd5(trimmed);
        }
        if (trimmed.indexOf('/') >= 0 || trimmed.indexOf(File.separatorChar) >= 0) {
            return shorten(trimmed, 96);
        }
        return shorten(trimmed, 80);
    }

    private static String shortMd5(String md5) {
        if (isBlank(md5)) {
            return "<empty>";
        }
        String value = md5.trim();
        if (value.length() <= 14) {
            return value;
        }
        return value.substring(0, 8) + "..." + value.substring(value.length() - 6);
    }

    private static Object loadEmojiInfoByMd5(ClassLoader classLoader, String emojiMd5) throws Exception {
        if (isBlank(emojiMd5)) {
            return null;
        }
        WeChatVersionSymbols.Profile symbols = weChatSymbols(classLoader);
        if (symbols.featureServiceSendStack) {
            Class<?> featureClass = findClass(classLoader, "com.tencent.mm.feature.emoji.b0");
            Object feature = findMethod(findVersionedClass(classLoader, "i95.n0"), "c", Class.class)
                    .invoke(null, featureClass);
            if (feature == null) {
                return null;
            }
            Object emojiMgr = findNoArgMethod(feature.getClass(), symbols.methods.emojiManager).invoke(feature);
            return emojiMgr == null
                    ? null
                    : findMethod(emojiMgr.getClass(), "y", String.class).invoke(emojiMgr, emojiMd5.trim());
        }
        Object storageRoot = findNoArgMethod(findClass(classLoader, "com.tencent.mm.storage.n5"), "f").invoke(null);
        Object storage = findNoArgMethod(storageRoot.getClass(), "c").invoke(storageRoot);
        Object emojiInfo = findMethod(storage.getClass(), "u1", String.class).invoke(storage, emojiMd5.trim());
        if (emojiInfo != null) {
            return emojiInfo;
        }
        try {
            Object service = findMethod(findVersionedClass(classLoader, "i95.n0"), "c", Class.class)
                    .invoke(null, findClass(classLoader, "k12.s"));
            if (service != null) {
                Object repository = findNoArgMethod(service.getClass(), "Bi").invoke(service);
                if (repository != null) {
                    return findMethod(repository.getClass(), "y", String.class).invoke(repository, emojiMd5.trim());
                }
            }
        } catch (Throwable t) {
            log("emoji repository fallback lookup skipped: " + shortError(t));
        }
        return null;
    }

    private static int emojiOrdinal(Object emojiInfo) {
        if (emojiInfo == null) {
            return 0;
        }
        try {
            Object value = findNoArgMethod(emojiInfo.getClass(), "c0").invoke(emojiInfo);
            if (value == null) {
                return 0;
            }
            Object ordinal = findNoArgMethod(value.getClass(), "ordinal").invoke(value);
            if (ordinal instanceof Number) {
                return ((Number) ordinal).intValue();
            }
        } catch (Throwable ignored) {
        }
        return 0;
    }

    private static void sendViaLocationFeature(ClassLoader classLoader, String wxid, double latitude, double longitude, int scale, String label, String poiName, String infoURL, String poiID, boolean fromPoiList, String poiTips) throws Exception {
        WeChatVersionSymbols.Profile symbols = weChatSymbols(classLoader);
        Class<?> paramsClass = findVersionedClass(classLoader, "y80.q1");
        Constructor<?> paramsCtor = findConstructor(paramsClass, String.class);
        Object params = paramsCtor.newInstance(wxid);
        // WeChat 8.0.78 inserted a new non-primitive field "d" into this class
        // (baseline "y80.q1", 8.0.77 name "na0.t1", 8.0.78 name "kb0.t1"), which
        // shifted every field below it one letter over. A naive "try candidate
        // A then candidate B" per field is NOT safe here because consecutive
        // fields' candidate lists overlap (e.g. latitude tries "d" then "e",
        // longitude tries "e" then "f") -- on the 8.0.78 layout latitude's
        // first candidate "d" is skipped (wrong type) so it lands on "e", then
        // longitude's own first candidate is also "e" and silently overwrites
        // the latitude value that was just set. Detect the layout once up
        // front instead, then assign each field by its one correct name.
        boolean legacyFieldLayout = isFieldOfType(params, "d", double.class);
        String latitudeField = legacyFieldLayout ? "d" : "e";
        String longitudeField = legacyFieldLayout ? "e" : "f";
        String scaleField = legacyFieldLayout ? "f" : "g";
        String labelField = legacyFieldLayout ? "g" : "h";
        String poiNameField = legacyFieldLayout ? "h" : "i";
        String infoURLField = legacyFieldLayout ? "i" : "j";
        String poiIDField = legacyFieldLayout ? "j" : "k";
        String fromPoiListField = legacyFieldLayout ? "k" : "l";
        String poiTipsField = legacyFieldLayout ? "l" : "m";
        setDoubleFieldAny(params, latitude, latitudeField);
        setDoubleFieldAny(params, longitude, longitudeField);
        setIntFieldAny(params, scale, scaleField);
        setObjectField(params, label == null ? "" : label, labelField);
        setObjectField(params, poiName == null ? "" : poiName, poiNameField);
        setObjectField(params, infoURL == null ? "" : infoURL, infoURLField);
        setObjectField(params, poiID == null ? "" : poiID, poiIDField);
        setBooleanFieldAny(params, fromPoiList, fromPoiListField);
        setObjectField(params, poiTips == null ? "" : poiTips, poiTipsField);

        Method getService = findMethod(findVersionedClass(classLoader, "i95.n0"), "c", Class.class);
        Object service = getService.invoke(null, findVersionedClass(classLoader, "z80.h0"));
        if (service == null) {
            service = getService.invoke(null, findVersionedClass(classLoader, "y80.p0"));
        }
        if (service == null) {
            throw new IllegalStateException("LocationMsgFeatureService is not available");
        }
        Method buildTask = findMethod(
                service.getClass(),
                symbols.methods.locationBuildTask,
                paramsClass);
        Object task = buildTask.invoke(service, params);
        if (task == null) {
            throw new IllegalStateException("location task build returned null");
        }
        Method send = findMethod(
                service.getClass(),
                symbols.methods.locationSendTask,
                findVersionedClass(classLoader, "qi3.b0"));
        send.invoke(service, task);
        log("sendLocation invoked LocationMsgSendTask scale=" + scale);
    }

    private static Object defaultVideoCrossParams(ClassLoader classLoader, Class<?> r2Class) throws Exception {
        WeChatVersionSymbols.Profile symbols = weChatSymbols(classLoader);
        Class<?> f7Class = findVersionedClass(classLoader, "c01.f7");
        Class<?> uf6Class = findVersionedClass(classLoader, "r45.uf6");
        Class<?> xz6Class = findVersionedClass(classLoader, "r45.xz6");
        Class<?> vh4Class = findVersionedClass(classLoader, "r45.vh4");
        Class<?> p2Class = findVersionedClass(classLoader, "vf0.p2");
        Class<?> r15dClass = findVersionedClass(classLoader, "r15.d");
        if (symbols.videoCrossParamsHasTrailingBoolean) {
            Constructor<?> ctor = findConstructor(
                    r2Class,
                    f7Class,
                    uf6Class,
                    String.class,
                    xz6Class,
                    String.class,
                    vh4Class,
                    boolean.class,
                    p2Class,
                    String.class,
                    r15dClass,
                    boolean.class,
                    boolean.class,
                    boolean.class);
            return ctor.newInstance(null, null, "", null, "", null, false, null, "", null, false, false, false);
        }
        Constructor<?> ctor = findConstructor(
                r2Class,
                f7Class,
                uf6Class,
                String.class,
                xz6Class,
                String.class,
                vh4Class,
                boolean.class,
                p2Class,
                String.class,
                r15dClass,
                boolean.class,
                boolean.class);
        return ctor.newInstance(null, null, "", null, "", null, false, null, "", null, false, false);
    }

    private static String videoFileNamePure(File videoFile) {
        String name = videoFile == null ? "" : videoFile.getName();
        int dot = name.lastIndexOf('.');
        if (dot > 0) {
            name = name.substring(0, dot);
        }
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < name.length(); i++) {
            char ch = name.charAt(i);
            if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9')) {
                out.append(ch);
            } else if (ch == '_' || ch == '-') {
                out.append(ch);
            }
        }
        if (out.length() == 0) {
            out.append("outbox_").append(System.currentTimeMillis());
        }
        return out.toString();
    }

    private static String voiceFileNamePure(File voiceFile) {
        String name = voiceFile == null ? "" : voiceFile.getName();
        int dot = name.lastIndexOf('.');
        if (dot > 0) {
            name = name.substring(0, dot);
        }
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < name.length() && out.length() < 32; i++) {
            char ch = name.charAt(i);
            if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9')) {
                out.append(ch);
            } else if (ch == '_' || ch == '-') {
                out.append(ch);
            }
        }
        if (out.length() == 0) {
            out.append("voice");
        }
        return "wo_voice_" + System.currentTimeMillis() + "_" + out;
    }

    private static Object ensureMessageDatabase(BridgeConfig config) {
        Object db = LAST_DATABASE;
        if (db != null && hasMessageTable(db)) {
            return db;
        }
        if (db != null) {
            log("cached WeChat database skipped: message table unavailable path=" + databasePath(db));
            if (LAST_DATABASE == db) {
                LAST_DATABASE = null;
            }
        }
        if (config == null) {
            return null;
        }
        db = findContactDatabaseOnMainThread(config);
        if (db != null && hasMessageTable(db)) {
            LAST_DATABASE = db;
            return db;
        }
        if (db != null) {
            log("discovered WeChat database skipped: message table unavailable path=" + databasePath(db));
            if (LAST_DATABASE == db) {
                LAST_DATABASE = null;
            }
        }
        return null;
    }

    private static boolean hasMessageTable(Object db) {
        if (db == null) {
            return false;
        }
        Object cursor = null;
        try {
            cursor = rawQuery(db, "SELECT 1 FROM message LIMIT 1", new String[]{});
            return cursor != null;
        } catch (Throwable ignored) {
            return false;
        } finally {
            closeQuietly(cursor);
        }
    }

    private static long waitForOutgoingImageMessage(BridgeConfig config, String wxid, long afterMsgId, long timeoutMs) {
        return waitForOutgoingMediaMessage(config, wxid, afterMsgId, timeoutMs, "image", "3");
    }

    private static long waitForOutgoingTextMessage(BridgeConfig config, String wxid, long afterMsgId, String text, long timeoutMs) {
        long deadline = System.currentTimeMillis() + Math.max(1000L, timeoutMs);
        Throwable lastError = null;
        while (System.currentTimeMillis() <= deadline) {
            try {
                Object db = ensureMessageDatabase(config);
                if (db != null) {
                    long msgId = findOutgoingTextMessageId(db, wxid, afterMsgId, text);
                    if (msgId > 0L) {
                        return msgId;
                    }
                }
            } catch (Throwable t) {
                lastError = t;
            }
            try {
                Thread.sleep(250L);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if (lastError != null) {
            log("text send DB verification failed: " + shortError(lastError));
        }
        return 0L;
    }

    private static long waitForOutgoingMediaMessage(BridgeConfig config, String wxid, long afterMsgId, long timeoutMs, String kind, String typeFilter) {
        long deadline = System.currentTimeMillis() + Math.max(1000L, timeoutMs);
        Throwable lastError = null;
        while (System.currentTimeMillis() <= deadline) {
            try {
                Object db = ensureMessageDatabase(config);
                if (db != null) {
                    long msgId = findOutgoingMediaMessageId(db, wxid, afterMsgId, typeFilter);
                    if (msgId > 0) {
                        return msgId;
                    }
                }
            } catch (Throwable t) {
                lastError = t;
            }
            try {
                Thread.sleep(350L);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if (lastError != null) {
            log(kind + " send DB verification failed: " + shortError(lastError));
        }
        return 0L;
    }

    private static long waitForOutgoingAppMsgMessage(BridgeConfig config, String wxid, long afterMsgId, long timeoutMs, String kind, int... appMsgTypes) {
        long deadline = System.currentTimeMillis() + Math.max(1000L, timeoutMs);
        Throwable lastError = null;
        while (System.currentTimeMillis() <= deadline) {
            try {
                Object db = ensureMessageDatabase(config);
                if (db != null) {
                    if (appMsgTypes != null) {
                        for (int appMsgType : appMsgTypes) {
                            long msgId = findOutgoingAppMsgMessageId(db, wxid, afterMsgId, appMsgType);
                            if (msgId > 0) {
                                return msgId;
                            }
                        }
                    }
                }
            } catch (Throwable t) {
                lastError = t;
            }
            try {
                Thread.sleep(350L);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if (lastError != null) {
            log(kind + " appmsg send DB verification failed: " + shortError(lastError));
        }
        return 0L;
    }

    private static void postVerifiedOutgoingMessage(BridgeConfig config, Object db, long msgId, String kind) {
        if (config == null || db == null || msgId <= 0L) {
            return;
        }
        try {
            MessagePayload payload = loadMessagePayloadById(config, db, msgId);
            if (payload == null) {
                log("send " + kind + " verified msgId=" + msgId + " but message row was not reportable");
                return;
            }
            post(config, payload);
            rememberMediaUpload(payload);
        } catch (Throwable t) {
            log("send " + kind + " verified message upload failed msgId=" + msgId + " error=" + shortError(t));
        }
    }

    private static MessagePayload loadMessagePayloadById(BridgeConfig config, Object db, long msgId) throws Exception {
        int queryMode = 2;
        Object cursor;
        try {
            cursor = rawQuery(db, ""
                    + "SELECT COALESCE(msgSvrId,0),talker,COALESCE(content,''),isSend,createTime,type,COALESCE(imgPath,'') "
                    + "FROM message "
                    + "WHERE msgId = ? AND talker IS NOT NULL AND talker <> '' "
                    + "LIMIT 1", new String[]{String.valueOf(msgId)});
        } catch (Throwable t) {
            try {
                queryMode = 1;
                cursor = rawQuery(db, ""
                        + "SELECT talker,COALESCE(content,''),isSend,createTime,type,COALESCE(imgPath,'') "
                        + "FROM message "
                        + "WHERE msgId = ? AND talker IS NOT NULL AND talker <> '' "
                        + "LIMIT 1", new String[]{String.valueOf(msgId)});
            } catch (Throwable ignored) {
                queryMode = 0;
                cursor = rawQuery(db, ""
                        + "SELECT talker,COALESCE(content,''),isSend,createTime,type "
                        + "FROM message "
                        + "WHERE msgId = ? AND talker IS NOT NULL AND talker <> '' "
                        + "LIMIT 1", new String[]{String.valueOf(msgId)});
            }
        }
        if (cursor == null) {
            return null;
        }
        try {
            Method moveToFirst = findNoArgMethod(cursor.getClass(), "moveToFirst");
            if (!Boolean.TRUE.equals(moveToFirst.invoke(cursor))) {
                return null;
            }
            int column = 0;
            long msgSvrId = queryMode == 2 ? longColumn(cursor, column++) : 0L;
            String talker = stringColumn(cursor, column++);
            String content = stringColumn(cursor, column++);
            int isSend = intColumn(cursor, column++);
            long createTime = normalizeCreateTime(longColumn(cursor, column++));
            int type = intColumn(cursor, column++);
            String imgPath = queryMode >= 1 ? stringColumn(cursor, column) : "";
            if (!shouldReportMessage(talker, content, type)) {
                return null;
            }
            int messageType = type <= 0 ? 1 : type;
            Long serverId = msgSvrId > 0L ? Long.valueOf(msgSvrId) : null;
            String mediaHint = resolveMediaHint(db, messageType, Long.valueOf(msgId), serverId, imgPath);
            return buildMessagePayload(config, talker, content, isSend == 1 ? Integer.valueOf(1) : Integer.valueOf(0), Long.valueOf(msgId), Long.valueOf(createTime), messageType, mediaHint);
        } finally {
            closeQuietly(cursor);
        }
    }

    private static long findOutgoingMediaMessageId(Object db, String wxid, long afterMsgId, String typeFilter) throws Exception {
        Object cursor = rawQuery(db, ""
                + "SELECT msgId "
                + "FROM message "
                + "WHERE msgId > ? AND talker = ? AND isSend = 1 AND type IN (" + typeFilter + ") "
                + "ORDER BY msgId DESC "
                + "LIMIT 1", new String[]{String.valueOf(afterMsgId), wxid});
        if (cursor == null) {
            return 0L;
        }
        try {
            Method moveToFirst = findNoArgMethod(cursor.getClass(), "moveToFirst");
            if (Boolean.TRUE.equals(moveToFirst.invoke(cursor))) {
                return longColumn(cursor, 0);
            }
            return 0L;
        } finally {
            closeQuietly(cursor);
        }
    }

    private static long findOutgoingTextMessageId(Object db, String wxid, long afterMsgId, String text) throws Exception {
        Object cursor = rawQuery(db, ""
                + "SELECT msgId "
                + "FROM message "
                + "WHERE msgId > ? AND talker = ? AND isSend = 1 AND type = 1 AND content = ? "
                + "ORDER BY msgId DESC "
                + "LIMIT 1", new String[]{String.valueOf(afterMsgId), wxid, text});
        if (cursor == null) {
            return 0L;
        }
        try {
            Method moveToFirst = findNoArgMethod(cursor.getClass(), "moveToFirst");
            if (Boolean.TRUE.equals(moveToFirst.invoke(cursor))) {
                return longColumn(cursor, 0);
            }
            return 0L;
        } finally {
            closeQuietly(cursor);
        }
    }

    private static long findOutgoingAppMsgMessageId(Object db, String wxid, long afterMsgId, int appMsgType) throws Exception {
        Object cursor = rawQuery(db, ""
                + "SELECT msgId "
                + "FROM message "
                + "WHERE msgId > ? AND talker = ? AND isSend = 1 AND type = ? AND content LIKE ? "
                + "ORDER BY msgId DESC "
                + "LIMIT 1", new String[]{
                String.valueOf(afterMsgId),
                wxid,
                String.valueOf(MESSAGE_TYPE_APPMSG),
                "%<type>" + appMsgType + "</type>%"});
        if (cursor == null) {
            return 0L;
        }
        try {
            Method moveToFirst = findNoArgMethod(cursor.getClass(), "moveToFirst");
            if (Boolean.TRUE.equals(moveToFirst.invoke(cursor))) {
                return longColumn(cursor, 0);
            }
            return 0L;
        } finally {
            closeQuietly(cursor);
        }
    }

    private static String normalizeRecordItemXML(String raw) {
        String value = raw == null ? "" : raw.trim();
        value = stripCData(value);
        String lower = value.toLowerCase(Locale.US);
        if (lower.startsWith("<recorditem")) {
            int start = value.indexOf('>');
            int end = lower.lastIndexOf("</recorditem>");
            if (start >= 0 && end > start) {
                value = value.substring(start + 1, end).trim();
                value = stripCData(value);
            }
        }
        return value.trim();
    }

    private static List<Long> readSourceChatRecordIds(JSONArray first, JSONArray second) {
        List<Long> ids = new ArrayList<>();
        Set<Long> seen = new HashSet<>();
        appendSourceChatRecordIds(ids, seen, first);
        appendSourceChatRecordIds(ids, seen, second);
        return ids;
    }

    private static void appendSourceChatRecordIds(List<Long> ids, Set<Long> seen, JSONArray values) {
        if (values == null) {
            return;
        }
        for (int i = 0; i < values.length() && ids.size() < MAX_CHAT_HISTORY_SOURCE_ITEMS; i++) {
            long id = values.optLong(i, 0L);
            if (id <= 0L || seen.contains(Long.valueOf(id))) {
                continue;
            }
            ids.add(Long.valueOf(id));
            seen.add(Long.valueOf(id));
        }
    }

    private static String buildChatHistoryRecordItemXML(Object db, List<Long> sourceMsgIds) throws Exception {
        if (sourceMsgIds == null || sourceMsgIds.isEmpty()) {
            return "";
        }
        List<ChatHistorySource> sources = new ArrayList<>();
        for (Long id : sourceMsgIds) {
            ChatHistorySource source = loadChatHistorySource(db, id == null ? 0L : id.longValue());
            if (source == null) {
                throw new IllegalStateException("source chat record was not found in local WeChat message database");
            }
            sources.add(source);
        }
        StringBuilder xml = new StringBuilder();
        xml.append("<recordinfo>");
        xml.append("<title>").append(xmlEscape("聊天记录")).append("</title>");
        xml.append("<desc>").append(xmlEscape("共" + sources.size() + "条")).append("</desc>");
        xml.append("<datalist count=\"").append(sources.size()).append("\">");
        int index = 0;
        for (ChatHistorySource source : sources) {
            index++;
            appendChatHistoryDataItem(xml, source, index);
        }
        xml.append("</datalist>");
        xml.append("</recordinfo>");
        String value = xml.toString();
        if (value.getBytes(StandardCharsets.UTF_8).length > 1024 * 1024) {
            throw new IllegalStateException("generated recorditem_xml exceeds 1MB");
        }
        return value;
    }

    private static ChatHistorySource loadChatHistorySource(Object db, long msgId) throws Exception {
        if (msgId <= 0L) {
            return null;
        }
        ChatHistorySource source = queryChatHistorySource(db, msgId, true);
        if (source != null) {
            return source;
        }
        return queryChatHistorySource(db, msgId, false);
    }

    private static ChatHistorySource queryChatHistorySource(Object db, long msgId, boolean includeMsgSource) throws Exception {
        String msgSourceSelect = includeMsgSource ? ",COALESCE(msgSource,'')" : ",''";
        Object cursor;
        try {
            cursor = rawQuery(db, ""
                    + "SELECT msgId,COALESCE(msgSvrId,0),talker,COALESCE(content,''),isSend,createTime,type"
                    + msgSourceSelect + " "
                    + "FROM message "
                    + "WHERE msgId = ? "
                    + "LIMIT 1", new String[]{String.valueOf(msgId)});
        } catch (Throwable t) {
            if (includeMsgSource) {
                return null;
            }
            throw t;
        }
        if (cursor == null) {
            return null;
        }
        try {
            Method moveToFirst = findNoArgMethod(cursor.getClass(), "moveToFirst");
            if (!Boolean.TRUE.equals(moveToFirst.invoke(cursor))) {
                return null;
            }
            ChatHistorySource source = new ChatHistorySource();
            int column = 0;
            source.msgId = longColumn(cursor, column++);
            source.msgSvrId = longColumn(cursor, column++);
            source.talker = stringColumn(cursor, column++);
            source.content = stringColumn(cursor, column++);
            source.isSend = intColumn(cursor, column++);
            source.createTime = normalizeCreateTime(longColumn(cursor, column++));
            source.type = intColumn(cursor, column++);
            source.msgSource = stringColumn(cursor, column);
            return source.msgId > 0L ? source : null;
        } finally {
            closeQuietly(cursor);
        }
    }

    private static void appendChatHistoryDataItem(StringBuilder xml, ChatHistorySource source, int index) {
        int dataType = chatHistoryDataType(source);
        String title = chatHistoryDataTitle(source);
        String description = chatHistoryDataDescription(source, title);
        xml.append("<dataitem datatype=\"").append(dataType).append("\" dataid=\"")
                .append(xmlEscape("msg_" + source.msgId + "_" + index)).append("\">");
        if (!isBlank(title)) {
            xml.append("<datatitle>").append(xmlEscape(title)).append("</datatitle>");
        }
        if (!isBlank(description)) {
            xml.append("<datadesc>").append(xmlEscape(description)).append("</datadesc>");
        }
        xml.append("<sourcetime>").append(source.createTime).append("</sourcetime>");
        xml.append("</dataitem>");
    }

    private static int chatHistoryDataType(ChatHistorySource source) {
        if (source == null) {
            return 1;
        }
        switch (source.type) {
            case 3:
                return 2;
            case 34:
                return 3;
            case 43:
            case 62:
                return 4;
            case 48:
                return 6;
            case 49:
                int appMsgType = appMsgTypeFromContent(source.content);
                if (appMsgType == 6) {
                    return 8;
                }
                if (appMsgType == 5) {
                    return 5;
                }
                return 1;
            default:
                return 1;
        }
    }

    private static String chatHistoryDataTitle(ChatHistorySource source) {
        if (source == null) {
            return "";
        }
        String text = normalizeMessageText(source.talker, source.content);
        switch (source.type) {
            case 1:
                return compactRecordText(text, 80);
            case 3:
                return "[图片]";
            case 34:
                return "[语音]";
            case 43:
            case 62:
                return "[视频]";
            case 47:
                return "[表情]";
            case 48:
                return "[位置]";
            case 49:
                return firstNonBlank(
                        compactRecordText(extractXmlField(text, "title"), 80),
                        compactRecordText(extractXmlField(text, "des"), 80),
                        "[链接/文件]");
            default:
                return compactRecordText(nonTextMessageDetail(source.type, text), 80);
        }
    }

    private static String chatHistoryDataDescription(ChatHistorySource source, String title) {
        if (source == null) {
            return "";
        }
        String text = normalizeMessageText(source.talker, source.content);
        if (source.type == 1) {
            return title;
        }
        if (source.type == 49) {
            return firstNonBlank(
                    compactRecordText(extractXmlField(text, "des"), 120),
                    compactRecordText(extractXmlField(text, "appname"), 80),
                    title);
        }
        return title;
    }

    private static int appMsgTypeFromContent(String content) {
        String value = extractXmlField(content, "type");
        if (isBlank(value)) {
            return 0;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (Throwable ignored) {
            return 0;
        }
    }

    private static String compactRecordText(String value, int limit) {
        if (isBlank(value)) {
            return "";
        }
        String text = value.trim().replace('\n', ' ').replace('\r', ' ').replace('\t', ' ');
        while (text.contains("  ")) {
            text = text.replace("  ", " ");
        }
        if (limit <= 0 || text.length() <= limit) {
            return text;
        }
        return text.substring(0, limit) + "...";
    }

    private static String xmlEscape(String value) {
        if (value == null) {
            return "";
        }
        return value
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&#39;");
    }

    private static String stripCData(String value) {
        value = value == null ? "" : value.trim();
        if (value.startsWith("<![CDATA[") && value.endsWith("]]>")) {
            return value.substring("<![CDATA[".length(), value.length() - "]]>".length()).trim();
        }
        return value;
    }

    private static void verifyWebSocketHandshake(InputStream input, String key) throws Exception {
        String headers = readHttpHeaders(input);
        String[] lines = headers.split("\r\n");
        if (lines.length == 0 || !lines[0].contains("101")) {
            throw new IOException("websocket upgrade failed: " + (lines.length == 0 ? "" : lines[0]));
        }
        String accept = "";
        for (String line : lines) {
            int index = line.indexOf(':');
            if (index <= 0) {
                continue;
            }
            String name = line.substring(0, index).trim();
            if ("Sec-WebSocket-Accept".equalsIgnoreCase(name)) {
                accept = line.substring(index + 1).trim();
            }
        }
        if (!webSocketAccept(key).equals(accept)) {
            throw new IOException("websocket accept mismatch");
        }
    }

    private static String readHttpHeaders(InputStream input) throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int matched = 0;
        int value;
        byte[] end = new byte[]{'\r', '\n', '\r', '\n'};
        while ((value = input.read()) != -1) {
            buffer.write(value);
            if ((byte) value == end[matched]) {
                matched++;
                if (matched == end.length) {
                    break;
                }
            } else {
                matched = (byte) value == end[0] ? 1 : 0;
            }
            if (buffer.size() > 32768) {
                throw new IOException("websocket headers too large");
            }
        }
        return new String(buffer.toByteArray(), StandardCharsets.ISO_8859_1);
    }

    private static String webSocketAccept(String key) throws Exception {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] digest = sha1.digest((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").getBytes(StandardCharsets.ISO_8859_1));
        return Base64.encodeToString(digest, Base64.NO_WRAP);
    }

    private static WebSocketFrame readWebSocketFrame(InputStream input) throws Exception {
        int first = input.read();
        int second = input.read();
        if (first < 0 || second < 0) {
            throw new IOException("websocket closed");
        }
        int opcode = first & 0x0F;
        boolean masked = (second & 0x80) != 0;
        long length = second & 0x7F;
        if (length == 126) {
            length = ((long) readByte(input) << 8) | readByte(input);
        } else if (length == 127) {
            length = 0L;
            for (int i = 0; i < 8; i++) {
                length = (length << 8) | readByte(input);
            }
        }
        if (length > 1024L * 1024L) {
            throw new IOException("websocket frame too large");
        }
        byte[] mask = null;
        if (masked) {
            mask = new byte[]{(byte) readByte(input), (byte) readByte(input), (byte) readByte(input), (byte) readByte(input)};
        }
        byte[] payload = new byte[(int) length];
        int offset = 0;
        while (offset < payload.length) {
            int read = input.read(payload, offset, payload.length - offset);
            if (read < 0) {
                throw new IOException("websocket payload truncated");
            }
            offset += read;
        }
        if (masked) {
            for (int i = 0; i < payload.length; i++) {
                payload[i] = (byte) (payload[i] ^ mask[i % 4]);
            }
        }
        return new WebSocketFrame(opcode, payload);
    }

    private static int readByte(InputStream input) throws Exception {
        int value = input.read();
        if (value < 0) {
            throw new IOException("websocket frame truncated");
        }
        return value & 0xFF;
    }

    private static void writeWebSocketText(OutputStream output, String text) throws Exception {
        writeWebSocketFrame(output, 0x1, text.getBytes(StandardCharsets.UTF_8));
    }

    private static void writeWebSocketFrame(OutputStream output, int opcode, byte[] payload) throws Exception {
        if (payload == null) {
            payload = new byte[0];
        }
        byte[] mask = new byte[4];
        new SecureRandom().nextBytes(mask);
        ByteArrayOutputStream frame = new ByteArrayOutputStream();
        frame.write(0x80 | (opcode & 0x0F));
        int length = payload.length;
        if (length < 126) {
            frame.write(0x80 | length);
        } else if (length <= 65535) {
            frame.write(0x80 | 126);
            frame.write((length >> 8) & 0xFF);
            frame.write(length & 0xFF);
        } else {
            frame.write(0x80 | 127);
            for (int i = 7; i >= 0; i--) {
                frame.write((int) (((long) length >> (8 * i)) & 0xFF));
            }
        }
        frame.write(mask);
        for (int i = 0; i < payload.length; i++) {
            frame.write(payload[i] ^ mask[i % 4]);
        }
        output.write(frame.toByteArray());
        output.flush();
    }

    private static boolean supportsPlainHttp(BridgeConfig config) {
        return config.baseUrl != null && config.baseUrl.trim().toLowerCase(Locale.US).startsWith("http://");
    }

    private static String urlEncode(String value) {
        if (value == null) {
            return "";
        }
        StringBuilder out = new StringBuilder(value.length() + 16);
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' || ch == '.' || ch == '~') {
                out.append(ch);
            } else {
                byte[] bytes = String.valueOf(ch).getBytes(StandardCharsets.UTF_8);
                for (byte b : bytes) {
                    out.append('%');
                    int v = b & 0xFF;
                    char high = Character.toUpperCase(Character.forDigit((v >> 4) & 0xF, 16));
                    char low = Character.toUpperCase(Character.forDigit(v & 0xF, 16));
                    out.append(high).append(low);
                }
            }
        }
        return out.toString();
    }

    private static void logWebSocketFailure(String message) {
        long now = System.currentTimeMillis();
        if (now - LAST_WEBSOCKET_FAIL_LOG_AT < 30000L) {
            return;
        }
        LAST_WEBSOCKET_FAIL_LOG_AT = now;
        log(message);
    }

    private static SendResult sendText(ClassLoader classLoader, String wxid, String text) {
        final ClassLoader targetClassLoader = classLoader;
        final String targetWxid = wxid;
        final String targetText = text;
        try {
            return callOnMainThread(new Callable<SendResult>() {
                @Override
                public SendResult call() {
                    return sendTextOnWeChatThread(targetClassLoader, targetWxid, targetText);
                }
            });
        } catch (Throwable t) {
            return SendResult.failed("WeChat send failed on main thread: " + shortError(t));
        }
    }

    private static SendResult sendTextOnWeChatThread(ClassLoader classLoader, String wxid, String text) {
        classLoader = runtimeClassLoader(classLoader);
        if (classLoader == null) {
            return SendResult.failed("WeChat classLoader is not available");
        }
        if (isBlank(wxid) || isBlank(text)) {
            return SendResult.failed("wxid and text are required");
        }

        BridgeConfig config = BridgeConfig.load(bridgeContext());
        Object db = ensureMessageDatabase(config);
        long beforeMsgId = 0L;
        if (db != null) {
            try {
                beforeMsgId = readMaxMessageId(db);
            } catch (Throwable t) {
                log("text send verification snapshot failed before send: " + shortError(t));
            }
        }

        int msgType = resolveMessageType(classLoader, wxid);
        Throwable builderUnavailable = null;
        Throwable directUnavailable = null;
        Throwable eventUnavailable = null;
        try {
            boolean sent = sendViaSendBuilder(classLoader, wxid, text, msgType);
            if (!sent) {
                // Was 3000ms, too short: under load the builder's send can still land
                // locally a few seconds later even after it returned false, and giving
                // up early here made us fall through to NetScene and send the same text
                // again for real -- producing real duplicate WeChat messages even though
                // the server-side outbox only ever leased/attempted the item once.
                // Match the "builder returned true" branch's 8000ms budget so a merely
                // slow builder send has the same chance to be recognized before we try
                // an independent fallback path.
                long msgId = waitForOutgoingTextMessage(config, wxid, beforeMsgId, text, 8000L);
                if (msgId > 0L) {
                    log("sendText builder returned false but outgoing text was verified msgId=" + msgId);
                    return SendResult.sent(msgId);
                }
                builderUnavailable = new IllegalStateException("WeChat send builder returned false");
                log("w11.r1 builder path returned false, trying w11.r0 NetScene");
            } else {
                log("sendText sent via w11.r1 builder wxid=" + wxid + " msgType=" + msgType);
                long msgId = waitForOutgoingTextMessage(config, wxid, beforeMsgId, text, 8000L);
                if (msgId > 0L) {
                    return SendResult.sent(msgId);
                }
                return SendResult.sent(0L);
            }
        } catch (Throwable t) {
            // Any failure here -- including an unexpected exception shape from a
            // per-version symbol that resolved to the wrong (but still loadable)
            // class, not just ClassNotFoundException/NoSuchMethodException -- should
            // fall through to the other independent send paths below rather than
            // give up immediately. This one previously short-circuited on anything
            // other than those two exception types, which meant a single wrong
            // guess for this path's symbols (w11.s1/aq1.l) could fail text sending
            // outright even though the w11.r0 NetScene path right below it, using a
            // completely different and already-confirmed-correct symbol, would have
            // worked.
            builderUnavailable = t;
            log("w11.r1 builder path failed, trying w11.r0 NetScene: " + shortError(t));
        }

        try {
            long msgId = sendViaNetScene(classLoader, wxid, text, msgType);
            log("sendText sent via w11.r0 NetScene wxid=" + wxid + " msgType=" + msgType + " msgId=" + msgId);
            return SendResult.sent(msgId);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            directUnavailable = e;
            log("w11.r0 NetScene path unavailable, trying dk5.s5.fj: " + shortError(e));
        } catch (Throwable t) {
            directUnavailable = t;
            log("w11.r0 NetScene send failed, trying dk5.s5.fj: " + shortError(t));
        }

        try {
            boolean published = sendViaSendMsgEvent(classLoader, wxid, text, msgType);
            if (!published) {
                throw new IllegalStateException("SendMsgEvent had no listener");
            }
            log("sendText published SendMsgEvent wxid=" + wxid + " msgType=" + msgType);
            return SendResult.sent(0L);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            eventUnavailable = e;
            log("SendMsgEvent path unavailable, trying dk5.s5.fj: " + shortError(e));
        } catch (Throwable t) {
            eventUnavailable = t;
            log("SendMsgEvent send failed, trying dk5.s5.fj: " + shortError(t));
        }

        try {
            sendViaSendMsgMgr(classLoader, wxid, text, msgType);
            log("sendText sent via dk5.s5.fj wxid=" + wxid + " msgType=" + msgType);
            return SendResult.sent(0L);
        } catch (Throwable t) {
            return SendResult.failed("WeChat send failed via dk5.s5.fj fallback: "
                    + shortError(t)
                    + "; event unavailable: " + shortError(eventUnavailable)
                    + "; direct unavailable: " + shortError(directUnavailable)
                    + "; builder unavailable: " + shortError(builderUnavailable));
        }
    }

    private static boolean sendViaSendMsgEvent(ClassLoader classLoader, String wxid, String text, int msgType) throws Exception {
        Object event = findClass(classLoader, "com.tencent.mm.autogen.events.SendMsgEvent")
                .getDeclaredConstructor()
                .newInstance();
        Field payloadField = findFieldAny(event.getClass(), "f71992g", "g");
        Object payload = payloadField.get(event);
        setObjectField(payload, wxid, "f7337a", "a");
        setObjectField(payload, text, "f7338b", "b");
        setIntFieldAny(payload, msgType, "f7339c", "c");
        setIntFieldAny(payload, 0, "f7340d", "d");
        Object result = findNoArgMethod(event.getClass(), "e").invoke(event);
        return result instanceof Boolean && (Boolean) result;
    }

    private static <T> T callOnMainThread(Callable<T> callable) throws Exception {
        if (Looper.myLooper() == Looper.getMainLooper()) {
            return callable.call();
        }
        FutureTask<T> task = new FutureTask<>(callable);
        new Handler(Looper.getMainLooper()).post(task);
        return task.get();
    }

    private static long sendViaNetScene(ClassLoader classLoader, String wxid, String text, int msgType) throws Exception {
        Class<?> netSceneClass = findVersionedClass(classLoader, "w11.r0");
        // WeChat 8.0.78's (String,String,int,int,long) constructor picked up a
        // trailing String parameter that 8.0.77 didn't have. Try the new
        // 8.0.78 shape first and fall back to the old one so this keeps
        // working on either version.
        Object scene;
        try {
            Constructor<?> ctor = findConstructor(netSceneClass, String.class, String.class, int.class, int.class, long.class, String.class);
            scene = ctor.newInstance(wxid, text, msgType, 4, 0L, "");
        } catch (NoSuchMethodException e) {
            Constructor<?> ctor = findConstructor(netSceneClass, String.class, String.class, int.class, int.class, long.class);
            scene = ctor.newInstance(wxid, text, msgType, 4, 0L);
        }
        long msgId = getLongField(scene, "f459357f", "f");
        if (msgId == -1L) {
            throw new IllegalStateException("w11.r0 inserted local msg failed");
        }

        Class<?> runCgi = findVersionedClass(classLoader, "com.tencent.mm.modelbase.z2");
        Object ready = findNoArgMethod(runCgi, "c").invoke(null);
        if (ready instanceof Boolean && !((Boolean) ready)) {
            throw new IllegalStateException("NetSceneQueue is not ready");
        }

        // z2.b(scene) (the "RunCgi.enqueue"-looking static helper) actually forwards to
        // NetSceneQueue.d(scene), which per the doc comment on resolveNetSceneQueueInstance
        // is CANCEL, not enqueue -- it never reached the network at all, it just
        // self-cancelled and relied on some independent, foreground-triggered "resend
        // unsent local rows" scan to eventually flush it once WeChat's UI was reopened.
        // enqueueNetScene (used for transfers) already uses the real entry point,
        // NetSceneQueue.h(scene, int); route text through the same one instead of z2.b().
        wakeNetSceneQueueForeground(classLoader);
        Object netSceneQueue = resolveNetSceneQueueInstance(classLoader);
        Method enqueue = findMethod(
                netSceneQueue.getClass(), "h", findVersionedClass(classLoader, "com.tencent.mm.modelbase.m1"), int.class);
        Object accepted = enqueue.invoke(netSceneQueue, scene, 0);
        if (accepted instanceof Boolean && !((Boolean) accepted)) {
            throw new IllegalStateException("NetSceneQueue rejected text send request");
        }
        return msgId;
    }

    private static void sendViaSendMsgMgr(ClassLoader classLoader, String wxid, String text, int msgType) throws Exception {
        WeChatVersionSymbols.Profile symbols = weChatSymbols(classLoader);
        Object service;
        if (symbols.featureServiceSendStack) {
            Class<?> serviceInterface = findVersionedClass(classLoader, "qh3.a1");
            service = findMethod(findVersionedClass(classLoader, "i95.n0"), "c", Class.class)
                    .invoke(null, serviceInterface);
        } else {
            Class<?> accessorClass = findClass(classLoader, "tg3.t1");
            Method accessor = findNoArgMethod(accessorClass, "a");
            service = accessor.invoke(null);
        }
        if (service == null) {
            throw new IllegalStateException("SendMsgMgr service is unavailable");
        }
        Method send = findMethod(service.getClass(), "fj", String.class, String.class, int.class, int.class);
        send.invoke(service, wxid, text, msgType, 0);
    }

    private static boolean sendViaSendBuilder(ClassLoader classLoader, String wxid, String text, int msgType) throws Exception {
        return sendViaSendBuilder(classLoader, wxid, text, msgType, null);
    }

    private static boolean sendViaSendBuilder(ClassLoader classLoader, String wxid, String text, int msgType, Object forwardInfo) throws Exception {
        ensureSendBuilderFactory(classLoader);
        Class<?> builderFactory = findVersionedClass(classLoader, "w11.s1");
        Method create = findMethod(builderFactory, "a", String.class);
        Object builder = create.invoke(null, wxid);
        if (builder == null) {
            throw new IllegalStateException("w11.s1.a returned null");
        }

        findMethod(builder.getClass(), "g", String.class).invoke(builder, wxid);
        findMethod(builder.getClass(), "e", String.class).invoke(builder, text);
        // WeChat 8.0.78 added a new h(String) setter on this builder, pushing
        // the existing int setter from "h" to "i".
        findMethodAny(builder.getClass(), new Class<?>[]{int.class}, "h", "i").invoke(builder, msgType);
        if (forwardInfo == null) {
            setForwardInfo(classLoader, builder);
        } else {
            applyForwardInfo(builder, forwardInfo);
        }
        setOptionalIntField(builder, 0, "f459371f", "f");
        setOptionalIntField(builder, 4, "f459374i", "i");

        Object request = findNoArgMethod(builder.getClass(), "a").invoke(builder);
        if (request == null) {
            throw new IllegalStateException("w11.r1.a returned null");
        }
        Object result = findNoArgMethod(request.getClass(), "a").invoke(request);
        return !(result instanceof Boolean) || (Boolean) result;
    }

    private static void ensureSendBuilderFactory(ClassLoader classLoader) throws Exception {
        Class<?> builderFactory = findVersionedClass(classLoader, "w11.s1");
        Field factoryField = findFieldAny(builderFactory, "f459386a", "a");
        if (factoryField.get(null) != null) {
            return;
        }
        Object factory = newInstanceAny(findVersionedClass(classLoader, "aq1.l"));
        factoryField.set(null, factory);
        log("initialized w11.s1 send factory with aq1.l");
    }

    private static void setForwardInfo(ClassLoader classLoader, Object builder) {
        try {
            applyForwardInfo(builder, createForwardInfo(classLoader));
        } catch (Throwable t) {
            log("forward info setup skipped: " + shortError(t));
        }
    }

    private static Object createForwardInfo(ClassLoader classLoader) throws Exception {
        return findVersionedClass(classLoader, "c01.h7").getDeclaredConstructor().newInstance();
    }

    private static void applyForwardInfo(Object builder, Object forwardInfo) throws Exception {
        findMethod(builder.getClass(), "f", forwardInfo.getClass()).invoke(builder, forwardInfo);
    }

    private static int resolveMessageType(ClassLoader classLoader, String wxid) {
        try {
            Class<?> typeResolver = findVersionedClass(classLoader, "c01.e2");
            Object result = findMethod(typeResolver, "C", String.class).invoke(null, wxid);
            if (result instanceof Number) {
                int value = ((Number) result).intValue();
                return value > 0 ? value : 1;
            }
            if (result != null) {
                int value = Integer.parseInt(String.valueOf(result));
                return value > 0 ? value : 1;
            }
        } catch (Throwable t) {
            log("resolve message type failed, fallback to text type 1: " + shortError(t));
        }
        return 1;
    }

    private static boolean isImageLikeMessageType(int type) {
        return type == 3 || type == 47;
    }

    private static Object resolveEventPayloadObject(Object event) throws Exception {
        if (event == null) {
            return null;
        }
        for (Class<?> current = event.getClass(); current != null && current != Object.class; current = current.getSuperclass()) {
            Field[] fields = current.getDeclaredFields();
            for (Field field : fields) {
                if (Modifier.isStatic(field.getModifiers()) || field.getType().isPrimitive()) {
                    continue;
                }
                field.setAccessible(true);
                Object value = field.get(event);
                if (value == null) {
                    Class<?> fieldType = field.getType();
                    if (!fieldType.isInterface() && !Modifier.isAbstract(fieldType.getModifiers())) {
                        try {
                            value = newInstanceAny(fieldType);
                            field.set(event, value);
                        } catch (Throwable ignored) {
                        }
                    }
                }
                if (value != null) {
                    return value;
                }
            }
        }
        return null;
    }

    private static void applyRevokePayload(Object payload, RevokeSource source, boolean isImage, Object messageRecord) throws Exception {
        if (payload == null || source == null) {
            throw new IllegalArgumentException("revoke payload and source are required");
        }
        if (messageRecord != null) {
            // messageRecord's runtime class can be a subclass of the field's
            // declared type, so this needs an assignability check rather than
            // collectInstanceFieldsByType's exact-type match.
            for (Class<?> current = payload.getClass(); current != null && current != Object.class; current = current.getSuperclass()) {
                for (Field field : current.getDeclaredFields()) {
                    if (Modifier.isStatic(field.getModifiers()) || !field.getType().isInstance(messageRecord)) {
                        continue;
                    }
                    field.setAccessible(true);
                    field.set(payload, messageRecord);
                }
            }
        }
        List<Field> stringFields = collectInstanceFieldsByType(payload.getClass(), String.class);
        List<Field> longFields = collectNumericInstanceFields(payload.getClass(), true);
        List<Field> intFields = collectNumericInstanceFields(payload.getClass(), false);
        List<Field> boolFields = collectInstanceFieldsByType(payload.getClass(), Boolean.TYPE, Boolean.class);

        if (!stringFields.isEmpty()) {
            stringFields.get(0).set(payload, firstNonBlank(source.talker, ""));
        }
        if (stringFields.size() > 1) {
            stringFields.get(1).set(payload, String.valueOf(source.msgId));
        }
        // WeChat 8.0.78's revoke handler range-checks whichever long field it
        // treats as the local "msgId" and rejects it if it looks like a large
        // msgSvrId-shaped number ("not in the reasonable scope") -- so the
        // small local msgId has to go in the first long field and the large
        // server-assigned msgSvrId in the second, the opposite of what this
        // code assumed before it was actually exercised against a real device.
        if (longFields.size() > 0) {
            setLongFieldValue(longFields.get(0), payload, source.msgId);
        }
        if (longFields.size() > 1) {
            setLongFieldValue(longFields.get(1), payload, firstPositiveLong(source.msgSvrId, source.msgId));
        }
        if (longFields.size() > 2) {
            setLongFieldValue(longFields.get(2), payload, source.createTime);
        }
        if (intFields.size() > 0) {
            setIntFieldValue(intFields.get(0), payload, safeLongToInt(firstPositiveLong(source.msgSvrId, source.msgId)));
        }
        if (intFields.size() > 1) {
            setIntFieldValue(intFields.get(1), payload, safeLongToInt(source.msgId));
        }
        if (intFields.size() > 2) {
            setIntFieldValue(intFields.get(2), payload, safeLongToInt(source.createTime));
        }
        if (!boolFields.isEmpty()) {
            Field field = boolFields.get(0);
            if (field.getType() == Boolean.class) {
                field.set(payload, Boolean.valueOf(isImage));
            } else {
                field.setBoolean(payload, isImage);
            }
        }
    }

    private static List<Field> collectInstanceFieldsByType(Class<?> cls, Class<?>... types) {
        List<Field> out = new ArrayList<>();
        if (cls == null || types == null || types.length == 0) {
            return out;
        }
        for (Class<?> current = cls; current != null && current != Object.class; current = current.getSuperclass()) {
            Field[] fields = current.getDeclaredFields();
            for (Field field : fields) {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                for (Class<?> type : types) {
                    if (field.getType() == type) {
                        field.setAccessible(true);
                        out.add(field);
                        break;
                    }
                }
            }
        }
        return out;
    }

    private static List<Field> collectNumericInstanceFields(Class<?> cls, boolean preferLong) {
        List<Field> out = new ArrayList<>();
        if (cls == null) {
            return out;
        }
        for (Class<?> current = cls; current != null && current != Object.class; current = current.getSuperclass()) {
            Field[] fields = current.getDeclaredFields();
            for (Field field : fields) {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                Class<?> type = field.getType();
                if (preferLong) {
                    if (type != Long.TYPE && type != Long.class) {
                        continue;
                    }
                } else if (type != Integer.TYPE && type != Integer.class) {
                    continue;
                }
                field.setAccessible(true);
                out.add(field);
            }
        }
        return out;
    }

    private static int safeLongToInt(long value) {
        if (value <= Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        }
        if (value >= Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) value;
    }

    private static void setLongFieldValue(Field field, Object target, long value) throws Exception {
        if (field.getType() == Long.class) {
            field.set(target, Long.valueOf(value));
            return;
        }
        field.setLong(target, value);
    }

    private static void setIntFieldValue(Field field, Object target, int value) throws Exception {
        if (field.getType() == Integer.class) {
            field.set(target, Integer.valueOf(value));
            return;
        }
        field.setInt(target, value);
    }

    private static Class<?> findClass(ClassLoader classLoader, String name) throws ClassNotFoundException {
        return Class.forName(name, false, classLoader);
    }

    /**
     * WeChat's R8 obfuscation reassigns short class names on every release build,
     * so a class name that is stable across one version bump can be reused for a
     * completely unrelated class in the next one (WeChat 8.0.78 renamed
     * platformtools.x2 to a3 and app.u5 to b6, while the old names got reassigned
     * to unrelated classes instead of simply disappearing). A plain candidate-name
     * loop that only checks ClassNotFoundException would silently resolve to the
     * wrong class in that case, so each candidate class name is paired with a
     * member name that must also resolve on it before it is accepted.
     */
    private static Class<?> findClassWithField(ClassLoader classLoader, String[] classCandidates, String... fieldCandidates) throws Exception {
        Exception lastError = null;
        for (String className : classCandidates) {
            try {
                Class<?> cls = findClass(classLoader, className);
                Field field = findFieldAny(cls, fieldCandidates);
                // A same-named field existing on an unrelated, R8-reassigned class is
                // exactly the trap this method exists to avoid (WeChat 8.0.78's "x2"
                // has an unrelated instance field "a", which a bare existence check
                // would happily accept). Callers always read these fields with
                // field.get(null), so require the match to actually be static.
                if (!java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                    lastError = new NoSuchFieldException(className + " field " + field.getName() + " is not static");
                    continue;
                }
                return cls;
            } catch (Exception e) {
                lastError = e;
            }
        }
        throw lastError != null ? lastError : new ClassNotFoundException("no candidate names given");
    }

    private static Class<?> findClassWithMethod(ClassLoader classLoader, String[] classCandidates, String methodName, Class<?>... paramTypes) throws Exception {
        Exception lastError = null;
        for (String className : classCandidates) {
            try {
                Class<?> cls = findClass(classLoader, className);
                Method method = findMethod(cls, methodName, paramTypes);
                if (!java.lang.reflect.Modifier.isStatic(method.getModifiers())) {
                    lastError = new NoSuchMethodException(className + " method " + method.getName() + " is not static");
                    continue;
                }
                return cls;
            } catch (Exception e) {
                lastError = e;
            }
        }
        throw lastError != null ? lastError : new ClassNotFoundException("no candidate names given");
    }

    private static Object newInstanceAny(Class<?> cls) throws Exception {
        try {
            Constructor<?> ctor = cls.getDeclaredConstructor();
            ctor.setAccessible(true);
            return ctor.newInstance();
        } catch (NoSuchMethodException ignored) {
            Constructor<?>[] constructors = cls.getDeclaredConstructors();
            for (Constructor<?> ctor : constructors) {
                ctor.setAccessible(true);
                try {
                    Class<?>[] types = ctor.getParameterTypes();
                    Object[] args = new Object[types.length];
                    for (int i = 0; i < types.length; i++) {
                        args[i] = defaultValue(types[i]);
                    }
                    return ctor.newInstance(args);
                } catch (Throwable t) {
                    log("constructor " + cls.getName() + "(" + ctor.getParameterCount() + ") failed: " + shortError(t));
                }
            }
            throw new NoSuchMethodException(cls.getName() + " constructors=" + describeConstructors(constructors));
        }
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0F;
        }
        if (type == double.class) {
            return 0D;
        }
        if (type == char.class) {
            return (char) 0;
        }
        return null;
    }

    private static Method findNoArgMethod(Class<?> cls, String name) throws NoSuchMethodException {
        return findMethod(cls, name);
    }

    private static Method findMethod(Class<?> cls, String name, Class<?>... parameterTypes) throws NoSuchMethodException {
        Class<?> current = cls;
        while (current != null) {
            try {
                Method method = current.getDeclaredMethod(name, parameterTypes);
                method.setAccessible(true);
                return method;
            } catch (NoSuchMethodException ignored) {
                current = current.getSuperclass();
            }
        }
        Method method = cls.getMethod(name, parameterTypes);
        method.setAccessible(true);
        return method;
    }

    private static Constructor<?> findConstructor(Class<?> cls, Class<?>... parameterTypes) throws NoSuchMethodException {
        Constructor<?> ctor = cls.getDeclaredConstructor(parameterTypes);
        ctor.setAccessible(true);
        return ctor;
    }

    private static void setIntField(Object target, String name, int value) throws Exception {
        Field field = findField(target.getClass(), name);
        field.setInt(target, value);
    }

    private static void setOptionalIntField(Object target, int value, String... names) {
        try {
            Field field = findFieldAny(target.getClass(), names);
            field.setInt(target, value);
        } catch (NoSuchFieldException e) {
            log("optional fields " + joinNames(names) + " not found on " + target.getClass().getName());
        } catch (Throwable t) {
            log("optional fields " + joinNames(names) + " set failed: " + shortError(t));
        }
    }

    /**
     * A name-only field lookup like findFieldAny is not safe for "try the 8.0.77
     * name, then the 8.0.78 name" fallbacks: WeChat 8.0.78 inserted a new
     * non-primitive field into com.tencent.mm.plugin.remittance... (baseline
     * "y80.q1"/kb0.t1)'s location params class, shifting every later field one
     * letter over. That means an 8.0.77 candidate name can still exist in 8.0.78,
     * just renamed to hold something of the wrong type (e.g. the old latitude
     * field "d" now holds an unrelated object), so the first candidate that
     * merely *exists* would be wrongly accepted and blow up on the type
     * mismatch instead of falling through to the next candidate. These helpers
     * therefore try every candidate name and only give up after all of them
     * fail (whether by missing name or by type mismatch).
     */
    private static void setIntFieldAny(Object target, int value, String... names) throws Exception {
        Exception last = null;
        for (String name : names) {
            try {
                findField(target.getClass(), name).setInt(target, value);
                return;
            } catch (Exception e) {
                last = e;
            }
        }
        throw last != null ? last : new NoSuchFieldException(joinNames(names));
    }

    private static void setLongFieldAny(Object target, long value, String... names) throws Exception {
        Exception last = null;
        for (String name : names) {
            try {
                findField(target.getClass(), name).setLong(target, value);
                return;
            } catch (Exception e) {
                last = e;
            }
        }
        throw last != null ? last : new NoSuchFieldException(joinNames(names));
    }

    private static void setDoubleFieldAny(Object target, double value, String... names) throws Exception {
        Exception last = null;
        for (String name : names) {
            try {
                findField(target.getClass(), name).setDouble(target, value);
                return;
            } catch (Exception e) {
                last = e;
            }
        }
        throw last != null ? last : new NoSuchFieldException(joinNames(names));
    }

    private static void setBooleanFieldAny(Object target, boolean value, String... names) throws Exception {
        Exception last = null;
        for (String name : names) {
            try {
                findField(target.getClass(), name).setBoolean(target, value);
                return;
            } catch (Exception e) {
                last = e;
            }
        }
        throw last != null ? last : new NoSuchFieldException(joinNames(names));
    }

    private static void setObjectField(Object target, Object value, String... names) throws Exception {
        Exception last = null;
        for (String name : names) {
            try {
                findField(target.getClass(), name).set(target, value);
                return;
            } catch (Exception e) {
                last = e;
            }
        }
        throw last != null ? last : new NoSuchFieldException(joinNames(names));
    }

    private static Field findFieldAny(Class<?> cls, String... names) throws NoSuchFieldException {
        NoSuchFieldException last = null;
        for (String name : names) {
            try {
                return findField(cls, name);
            } catch (NoSuchFieldException e) {
                last = e;
            }
        }
        throw last == null ? new NoSuchFieldException("") : last;
    }

    private static Method findMethodAny(Class<?> cls, Class<?>[] parameterTypes, String... names) throws NoSuchMethodException {
        NoSuchMethodException last = null;
        for (String name : names) {
            try {
                return findMethod(cls, name, parameterTypes);
            } catch (NoSuchMethodException e) {
                last = e;
            }
        }
        throw last == null ? new NoSuchMethodException("") : last;
    }

    private static long getLongField(Object target, String... names) throws Exception {
        Field field = findFieldAny(target.getClass(), names);
        Object value = field.get(target);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return Long.parseLong(String.valueOf(value));
    }

    private static String getStringField(Object target, String... names) throws Exception {
        Field field = findFieldAny(target.getClass(), names);
        Object value = field.get(target);
        return value == null ? "" : String.valueOf(value);
    }

    private static String joinNames(String... names) {
        StringBuilder out = new StringBuilder();
        for (String name : names) {
            if (out.length() > 0) {
                out.append("/");
            }
            out.append(name);
        }
        return out.toString();
    }

    private static String describeConstructors(Constructor<?>[] constructors) {
        if (constructors == null || constructors.length == 0) {
            return "[]";
        }
        StringBuilder out = new StringBuilder("[");
        for (int i = 0; i < constructors.length; i++) {
            if (i > 0) {
                out.append(", ");
            }
            Class<?>[] types = constructors[i].getParameterTypes();
            out.append("(");
            for (int j = 0; j < types.length; j++) {
                if (j > 0) {
                    out.append(",");
                }
                out.append(types[j].getName());
            }
            out.append(")");
        }
        out.append("]");
        return out.toString();
    }

    private static Field findField(Class<?> cls, String name) throws NoSuchFieldException {
        Class<?> current = cls;
        while (current != null) {
            try {
                Field field = current.getDeclaredField(name);
                field.setAccessible(true);
                return field;
            } catch (NoSuchFieldException ignored) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name);
    }

    private static boolean isFieldOfType(Object target, String name, Class<?> type) {
        try {
            return findField(target.getClass(), name).getType() == type;
        } catch (Throwable t) {
            return false;
        }
    }

    private static String postJson(BridgeConfig config, String path, String bodyJson) throws Exception {
        if (config.baseUrl != null && config.baseUrl.trim().toLowerCase(Locale.US).startsWith("http://")) {
            return postJsonSocket(config, path, bodyJson);
        }
        try {
            return postJsonOnce(config, path, bodyJson);
        } catch (IOException first) {
            log("postJson retry after io error: " + shortError(first));
            System.setProperty("http.keepAlive", "false");
            return postJsonOnce(config, path, bodyJson);
        }
    }

    private static String postJsonSocket(BridgeConfig config, String path, String bodyJson) throws Exception {
        URL url = new URL(trimRight(config.baseUrl, "/") + path);
        String requestPath = url.getFile();
        if (isBlank(requestPath)) {
            requestPath = "/";
        }
        int port = url.getPort() > 0 ? url.getPort() : 80;
        String host = url.getHost();
        String hostHeader = url.getPort() > 0 ? host + ":" + port : host;
        byte[] body = bodyJson.getBytes(StandardCharsets.UTF_8);
        log("postJsonSocket path=" + path + " host=" + hostHeader + " bodyBytes=" + body.length);

        StringBuilder headers = new StringBuilder();
        headers.append("POST ").append(requestPath).append(" HTTP/1.1\r\n");
        headers.append("Host: ").append(hostHeader).append("\r\n");
        headers.append("User-Agent: WechatObservatoryModule/0.1\r\n");
        headers.append("Accept: application/json\r\n");
        headers.append("Content-Type: application/json; charset=utf-8\r\n");
        headers.append("Content-Length: ").append(body.length).append("\r\n");
        headers.append("Connection: close\r\n\r\n");

        ByteArrayOutputStream response = new ByteArrayOutputStream();
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 5000);
            socket.setSoTimeout(5000);
            OutputStream out = socket.getOutputStream();
            out.write(headers.toString().getBytes(StandardCharsets.ISO_8859_1));
            out.write(body);
            out.flush();
            socket.shutdownOutput();

            InputStream input = socket.getInputStream();
            byte[] buffer = new byte[4096];
            int read;
            while ((read = input.read(buffer)) != -1) {
                response.write(buffer, 0, read);
            }
        }

        byte[] responseBytes = response.toByteArray();
        if (responseBytes.length == 0) {
            throw new IOException("bridge returned empty socket response");
        }
        String responseText = new String(responseBytes, StandardCharsets.ISO_8859_1);
        int headerEnd = responseText.indexOf("\r\n\r\n");
        if (headerEnd < 0) {
            throw new IOException("bridge returned malformed socket response");
        }
        int statusEnd = responseText.indexOf("\r\n");
        String statusLine = statusEnd < 0 ? responseText : responseText.substring(0, statusEnd);
        String[] statusParts = statusLine.split(" ");
        int status = statusParts.length >= 2 ? Integer.parseInt(statusParts[1]) : 0;
        byte[] bodyBytes = new byte[responseBytes.length - headerEnd - 4];
        System.arraycopy(responseBytes, headerEnd + 4, bodyBytes, 0, bodyBytes.length);

        String headersText = responseText.substring(0, headerEnd).toLowerCase(Locale.US);
        String responseBody = headersText.contains("transfer-encoding: chunked")
                ? decodeChunkedBody(bodyBytes)
                : new String(bodyBytes, StandardCharsets.UTF_8);
        if (status < 200 || status >= 300) {
            throw new IllegalStateException("bridge returned HTTP " + status + ": " + responseBody);
        }
        return responseBody;
    }

    private static String decodeChunkedBody(byte[] bodyBytes) throws Exception {
        ByteArrayOutputStream decoded = new ByteArrayOutputStream();
        int offset = 0;
        while (offset < bodyBytes.length) {
            int lineEnd = indexOfCrlf(bodyBytes, offset);
            if (lineEnd < 0) {
                break;
            }
            String sizeLine = new String(bodyBytes, offset, lineEnd - offset, StandardCharsets.ISO_8859_1).trim();
            int semicolon = sizeLine.indexOf(';');
            if (semicolon >= 0) {
                sizeLine = sizeLine.substring(0, semicolon);
            }
            int chunkSize = Integer.parseInt(sizeLine, 16);
            offset = lineEnd + 2;
            if (chunkSize == 0) {
                break;
            }
            if (offset + chunkSize > bodyBytes.length) {
                throw new IOException("truncated chunked response");
            }
            decoded.write(bodyBytes, offset, chunkSize);
            offset += chunkSize + 2;
        }
        return new String(decoded.toByteArray(), StandardCharsets.UTF_8);
    }

    private static int indexOfCrlf(byte[] bytes, int start) {
        for (int i = start; i + 1 < bytes.length; i++) {
            if (bytes[i] == '\r' && bytes[i + 1] == '\n') {
                return i;
            }
        }
        return -1;
    }

    private static String postJsonOnce(BridgeConfig config, String path, String bodyJson) throws Exception {
        URL url = new URL(trimRight(config.baseUrl, "/") + path);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/json; charset=utf-8");
        connection.setRequestProperty("Connection", "close");

        byte[] body = bodyJson.getBytes(StandardCharsets.UTF_8);
        connection.setFixedLengthStreamingMode(body.length);
        try (OutputStream out = connection.getOutputStream()) {
            out.write(body);
        }

        int status = connection.getResponseCode();
        String response = readResponse(status >= 200 && status < 300 ? connection.getInputStream() : connection.getErrorStream());
        connection.disconnect();
        if (status < 200 || status >= 300) {
            throw new IllegalStateException("bridge returned HTTP " + status + ": " + response);
        }
        return response;
    }

    private static String readResponse(InputStream input) throws Exception {
        if (input == null) {
            return "";
        }
        StringBuilder out = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                out.append(line);
            }
        }
        return out.toString();
    }

    private static String stringArg(Object[] args, int index) {
        if (args == null || args.length <= index || args[index] == null) {
            return "";
        }
        return String.valueOf(args[index]);
    }

    private static long normalizeCreateTime(Long createTime) {
        return normalizeCreateTime(createTime == null ? 0L : createTime.longValue());
    }

    private static long normalizeCreateTime(long createTime) {
        if (createTime <= 0) {
            return System.currentTimeMillis() / 1000L;
        }
        return createTime > 10_000_000_000L ? createTime / 1000L : createTime;
    }

    private static void log(String message) {
        BridgeLogger.log(message);
    }

}
