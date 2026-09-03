package bridge

import "testing"

func TestTransferAcceptValidationBindsOnlySourceMessage(t *testing.T) {
	req, err := (SendActionRequest{
		Device:       "phone-a",
		OwnerWxID:    "wxid_self",
		Kind:         OutboxKindTransferAccept,
		ChatRecordID: 12345,
	}).Validate("")
	if err != nil {
		t.Fatalf("transfer accept should not require caller wx_ids: %v", err)
	}
	if len(req.WxIDs) != 1 || req.WxIDs[0] != "" || req.SourceChatRecordID != 12345 {
		t.Fatalf("transfer accept must be source-bound: %+v", req)
	}
}

func TestTransferAcceptValidationRequiresSourceMessage(t *testing.T) {
	_, err := (SendActionRequest{
		Device: "phone-a",
		Kind:   OutboxKindTransferAccept,
	}).Validate("")
	if err == nil || err.Error() != "chat_record_id is required" {
		t.Fatalf("expected chat_record_id validation error, got %v", err)
	}
}

func TestPublicTransferEnvelopeKeepsStructuredFieldsAndDropsRawXML(t *testing.T) {
	envelope := publicEventMessageEnvelope(MessageEvent{
		Device:       "phone-a",
		Direction:    DirectionRecv,
		From:         "wxid_payer",
		To:           "wxid_self",
		Text:         "[转账]",
		MessageType:  MessageTypeTransfer,
		RawXML:       "<secret />",
		ChatRecordID: 99,
		Payment: &PaymentInfo{
			Type:          "transfer",
			State:         "pending",
			AmountMinor:   1234,
			Currency:      "CNY",
			TransactionID: "transaction-1",
			TransferID:    "transfer-1",
		},
	})
	if envelope.Payment == nil || envelope.Payment.AmountMinor != 1234 || envelope.Payment.State != "pending" {
		t.Fatalf("structured payment envelope missing: %+v", envelope)
	}
}
