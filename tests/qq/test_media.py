import base64

import pytest

from shittim_core.domain.types import ConversationAddress, InputEvent, Segment
from shittim_core.qq.codec import media_segment, plan_send, resolve_files


@pytest.mark.parametrize("kind", ["image", "record", "video"])
def test_astrbot_media_encoding_keeps_bytes_and_route(kind, tmp_path):
    video = tmp_path / "video.mp4"
    segment = media_segment(kind, b"fixture bytes", str(video))
    send = plan_send(ConversationAddress("123", "group", "789"), [segment])
    assert send[0].action == "send_group_msg"
    encoded = send[0].params["message"][0]
    assert encoded["type"] == kind
    if kind != "video":
        assert (
            base64.b64decode(encoded["data"]["file"].removeprefix("base64://")) == b"fixture bytes"
        )
    else:
        assert encoded["data"]["file"] == video.as_uri()


async def test_media_intake_never_interprets_protocol_path_as_local_path():
    event = InputEvent(
        ConversationAddress("123", "private", "456"),
        "456",
        "1",
        "",
        (
            Segment("image", {"file": "/etc/private"}),
            Segment("record", {"url": "https://files.test/audio.wav"}),
        ),
    )

    async def no_call(*args):
        raise AssertionError("Media URL cannot invoke an unrelated file action")

    results = await resolve_files(event, no_call)
    assert results[0].status == "unavailable" and results[0].media_type == "image"
    assert results[1].status == "located" and results[1].media_type == "record"
