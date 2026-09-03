package cc.wechat.observatory;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TransferMessageParserTest {
    @Test
    public void parsesPendingIncomingTransfer() {
        String xml = "<msg><appmsg><type>2000</type><wcpayinfo>"
                + "<paysubtype>1</paysubtype><feedesc><![CDATA[￥12.34]]></feedesc>"
                + "<transcationid>tx-1</transcationid><transferid>tf-1</transferid>"
                + "<invalidtime>86400</invalidtime><effectivedate>1</effectivedate>"
                + "<total_fee>1234</total_fee><fee_type>CNY</fee_type>"
                + "<pay_memo>晚饭</pay_memo><payer_username>wxid_payer</payer_username>"
                + "<receiver_username>wxid_receiver</receiver_username><attach>a&amp;b</attach>"
                + "</wcpayinfo></appmsg></msg>";

        TransferMessageParser.Result result = TransferMessageParser.parse(xml);

        assertTrue(result.isTransfer());
        assertTrue(result.isPending());
        assertTrue(result.hasConfirmFields());
        assertEquals(1234L, result.amountMinor);
        assertEquals("CNY", result.currency);
        assertEquals("a&b", result.attach);
    }

    @Test
    public void rejectsAlreadyReceivedTransfer() {
        String xml = "<appmsg><type>2000</type><wcpayinfo><paysubtype>1</paysubtype>"
                + "<receivestatus>3</receivestatus><transcationid>tx</transcationid>"
                + "<transferid>tf</transferid><total_fee>1</total_fee></wcpayinfo></appmsg>";

        TransferMessageParser.Result result = TransferMessageParser.parse(xml);

        assertFalse(result.isPending());
        assertEquals("received", result.state);
    }

    @Test
    public void derivesMinorUnitsFromCnyDisplayAmountWhenTotalFeeIsMissing() {
        String xml = "<msg><appmsg><type>2000</type><wcpayinfo>"
                + "<paysubtype>1</paysubtype><feedesc><![CDATA[￥1,234.56]]></feedesc>"
                + "<transcationid>tx</transcationid><transferid>tf</transferid>"
                + "</wcpayinfo></appmsg></msg>";

        TransferMessageParser.Result result = TransferMessageParser.parse(xml);

        assertTrue(result.isPending());
        assertTrue(result.hasConfirmFields());
        assertEquals(123456L, result.amountMinor);
        assertEquals("CNY", result.currency);
    }

    @Test
    public void doesNotGuessMinorUnitsForUnknownDisplayCurrency() {
        String xml = "<msg><appmsg><type>2000</type><wcpayinfo>"
                + "<paysubtype>1</paysubtype><feedesc><![CDATA[$12.34]]></feedesc>"
                + "<transcationid>tx</transcationid><transferid>tf</transferid>"
                + "</wcpayinfo></appmsg></msg>";

        TransferMessageParser.Result result = TransferMessageParser.parse(xml);

        assertFalse(result.hasConfirmFields());
        assertEquals(0L, result.amountMinor);
    }
}
