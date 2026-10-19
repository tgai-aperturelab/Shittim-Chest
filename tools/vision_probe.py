"""A synthetic randomized image checks actual image input, without private user material."""

import base64
import re
import secrets
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from shittim_core.agent.model import OpenAIModel


async def run_vision_probe(
    directory: Path, *, base_url: str, model_name: str, api_key: str
) -> dict:
    directory.mkdir(parents=True, exist_ok=False)
    digits = str(100000 + secrets.randbelow(900000))
    image = Image.new("RGB", (500, 160), "white")
    font_path = Path("C:/Windows/Fonts/arial.ttf")
    font = (
        ImageFont.truetype(str(font_path), 86)
        if font_path.exists()
        else ImageFont.load_default(size=70)
    )
    ImageDraw.Draw(image).text((30, 25), digits, fill="black", font=font)
    target = directory / "challenge.png"
    image.save(target)
    model = OpenAIModel(
        model=model_name,
        base_url=base_url,
        api_key=api_key,
        max_tokens=2048,
        timeout=90,
        reasoning_echo=True,
    )
    result = {
        "kind": "real_model_vision_probe",
        "requested_model": model_name,
        "expected_digits": digits,
        "result": "fail",
        "qq_live": False,
    }
    try:
        response = await model.complete(
            [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "请只输出图片中的六位数字。"},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": "data:image/png;base64,"
                                + base64.b64encode(target.read_bytes()).decode()
                            },
                        },
                    ],
                }
            ],
            [],
        )
        result["answer"] = response.content
        result["finish_reason"] = response.finish_reason
        if re.findall(r"\d{6}", response.content or "") == [digits]:
            result["result"] = "pass"
    except Exception as error:
        result["error_type"] = type(error).__name__
        result["status_code"] = getattr(error, "status_code", None)
    finally:
        await model.close()
    return result
