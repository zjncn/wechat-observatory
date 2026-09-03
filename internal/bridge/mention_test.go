package bridge

import "testing"

func TestNormalizePreservesSafeAtSelfEvidence(t *testing.T) {
	event := MessageEvent{
		Device:      "phone-a",
		Direction:   DirectionRecv,
		ChatKind:    ChatKindRoom,
		RoomID:      "room@chatroom",
		From:        "room@chatroom",
		To:          "wxid_self",
		Sender:      "wxid_sender",
		Text:        "@display hello",
		AtSelf:      true,
		MessageType: 1,
	}.Normalize()

	if !containsString(event.Evidence, "msg_source.at_self") {
		t.Fatalf("at_self evidence missing: %#v", event.Evidence)
	}
	public := publicEventMessageEnvelope(event)
	if !public.AtSelf {
		t.Fatal("public envelope should expose at_self")
	}
}

func TestNormalizeDoesNotPromoteDirectAtSelf(t *testing.T) {
	event := MessageEvent{
		Device:      "phone-a",
		Direction:   DirectionRecv,
		ChatKind:    ChatKindDirect,
		From:        "wxid_friend",
		To:          "wxid_self",
		Text:        "hello",
		AtSelf:      true,
		MessageType: 1,
	}.Normalize()

	if containsString(event.Evidence, "msg_source.at_self") {
		t.Fatal("direct messages must not gain group mention evidence")
	}
}
