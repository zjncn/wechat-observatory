/*
 * @input WeChat appmsg XML containing wcpayinfo
 * @output Validated transfer identifiers, amount, parties, state, and confirm parameters
 * @position Pure-Java boundary parser shared by inbound reporting and transfer acceptance
 * @auto-doc Update header and folder INDEX.md when this file changes
 */
package cc.wechat.observatory;

import java.math.BigDecimal;
import java.util.Locale;

final class TransferMessageParser {
    static final int APPMSG_TYPE_TRANSFER = 2000;

    private TransferMessageParser() {
    }

    static Result parse(String xml) {
        Result result = new Result();
        result.appMsgType = parseInt(tag(xml, "type"));
        result.paySubtype = parseInt(tag(xml, "paysubtype"));
        result.displayAmount = tag(xml, "feedesc");
        result.transactionId = tag(xml, "transcationid");
        result.transferId = tag(xml, "transferid");
        result.invalidTime = parseInt(tag(xml, "invalidtime"));
        result.effectiveDate = parseInt(tag(xml, "effectivedate"));
        result.amountMinor = parseLong(tag(xml, "total_fee"));
        result.currency = tag(xml, "fee_type");
        if (result.amountMinor <= 0L) {
            result.amountMinor = parseCnyDisplayAmountMinor(result.displayAmount);
            if (result.amountMinor > 0L && result.currency.isEmpty()) {
                result.currency = "CNY";
            }
        }
        result.memo = tag(xml, "pay_memo");
        result.payerWxid = tag(xml, "payer_username");
        result.receiverWxid = tag(xml, "receiver_username");
        result.receiveStatus = parseInt(tag(xml, "receivestatus"));
        result.attach = tag(xml, "attach");
        result.state = state(result.paySubtype, result.receiveStatus);
        return result;
    }

    private static String tag(String xml, String name) {
        if (xml == null || name == null) {
            return "";
        }
        String lower = xml.toLowerCase(Locale.US);
        String open = "<" + name.toLowerCase(Locale.US) + ">";
        String close = "</" + name.toLowerCase(Locale.US) + ">";
        int start = lower.indexOf(open);
        if (start < 0) {
            return "";
        }
        start += open.length();
        int end = lower.indexOf(close, start);
        if (end < start) {
            return "";
        }
        String value = xml.substring(start, end).trim();
        if (value.startsWith("<![CDATA[") && value.endsWith("]]>") && value.length() >= 12) {
            value = value.substring(9, value.length() - 3).trim();
        }
        return xmlUnescape(value).trim();
    }

    private static String xmlUnescape(String value) {
        return value.replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&quot;", "\"")
                .replace("&apos;", "'")
                .replace("&amp;", "&");
    }

    private static int parseInt(String value) {
        try {
            return Integer.parseInt(value.trim());
        } catch (Throwable ignored) {
            return 0;
        }
    }

    private static long parseLong(String value) {
        try {
            return Long.parseLong(value.trim());
        } catch (Throwable ignored) {
            return 0L;
        }
    }

    private static long parseCnyDisplayAmountMinor(String value) {
        if (value == null) {
            return 0L;
        }
        String text = value.trim();
        if (!(text.contains("￥") || text.contains("¥")
                || text.toUpperCase(Locale.US).contains("CNY") || text.contains("元"))) {
            return 0L;
        }
        StringBuilder number = new StringBuilder();
        boolean decimalSeen = false;
        boolean started = false;
        for (int i = 0; i < text.length(); i++) {
            char current = text.charAt(i);
            if (current >= '0' && current <= '9') {
                number.append(current);
                started = true;
            } else if (current == '.' && started && !decimalSeen) {
                number.append(current);
                decimalSeen = true;
            } else if (current == ',' && started) {
                continue;
            } else if (started) {
                break;
            }
        }
        if (number.length() == 0 || number.charAt(number.length() - 1) == '.') {
            return 0L;
        }
        try {
            return new BigDecimal(number.toString()).movePointRight(2).longValueExact();
        } catch (Throwable ignored) {
            return 0L;
        }
    }

    private static String state(int paySubtype, int receiveStatus) {
        switch (receiveStatus) {
            case 3:
                return "received";
            case 4:
                return "refused";
            case 2:
                return "expired";
            case 5:
            case 6:
                return "closed";
            default:
                return paySubtype == 1 ? "pending" : "unknown";
        }
    }

    static final class Result {
        int appMsgType;
        int paySubtype;
        String displayAmount = "";
        String transactionId = "";
        String transferId = "";
        int invalidTime;
        int effectiveDate;
        long amountMinor;
        String currency = "";
        String memo = "";
        String payerWxid = "";
        String receiverWxid = "";
        int receiveStatus;
        String attach = "";
        String state = "unknown";

        boolean isTransfer() {
            return appMsgType == APPMSG_TYPE_TRANSFER || (!transactionId.isEmpty() && !transferId.isEmpty());
        }

        boolean isPending() {
            return isTransfer() && paySubtype == 1 && "pending".equals(state);
        }

        boolean hasConfirmFields() {
            return !transactionId.isEmpty()
                    && !transferId.isEmpty()
                    && amountMinor > 0L
                    && amountMinor <= Integer.MAX_VALUE;
        }
    }
}
