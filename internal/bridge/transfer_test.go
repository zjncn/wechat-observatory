package bridge

import "testing"

func TestAckOutboxMarksTransferDeclinedOnSentAck(t *testing.T) {
	outbox := &fakeOutbox{}
	persistence := &fakePersistence{}
	service := newTestService("", WithOutbox(outbox), WithPersistence(persistence))

	item, err := outbox.EnqueueReply(t.Context(), ReplyAction{
		Device:    "phone-a",
		OwnerWxID: "wxid_self",
		WxID:      "",
		Kind:      OutboxKindTransferDecline,
		Text:      "[拒收转账]",
	})
	if err != nil {
		t.Fatal(err)
	}

	acked, err := service.AckOutbox(t.Context(), ModuleAckRequest{
		APIKey: testAPIKey,
		Device: "phone-a",
		WxID:   "wxid_self",
		Items: []ModuleAckItem{{
			ID:           item.ID,
			Status:       "sent",
			ChatRecordID: 67168,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(acked) != 1 || acked[0].Status != "sent" {
		t.Fatalf("expected transfer decline ack to stay sent, got %+v", acked)
	}
	if len(persistence.markedDeclined) != 1 || persistence.markedDeclined[0] != 67168 {
		t.Fatalf("expected MarkTransferDeclined to be called with chat_record_id=67168, got %+v", persistence.markedDeclined)
	}
}

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

func TestTransferDeclineValidationBindsOnlySourceMessage(t *testing.T) {
	req, err := (SendActionRequest{
		Device:       "phone-a",
		OwnerWxID:    "wxid_self",
		Kind:         OutboxKindTransferDecline,
		ChatRecordID: 12345,
	}).Validate("")
	if err != nil {
		t.Fatalf("transfer decline should not require caller wx_ids: %v", err)
	}
	if len(req.WxIDs) != 1 || req.WxIDs[0] != "" || req.SourceChatRecordID != 12345 {
		t.Fatalf("transfer decline must be source-bound: %+v", req)
	}
}

func TestTransferDeclineValidationRequiresSourceMessage(t *testing.T) {
	_, err := (SendActionRequest{
		Device: "phone-a",
		Kind:   OutboxKindTransferDecline,
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
