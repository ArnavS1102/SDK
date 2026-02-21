"""
Gemini API client for Vertex AI.

Provides:
- Text/analysis: image-to-text (gemini-2.5-pro)
- Image generation: image-to-image (gemini-2.5-flash-image)
"""

from __future__ import annotations

import base64
import json
import os
import re
from typing import List, Optional, Union

import requests

# Optional: google-auth for Vertex AI ADC
try:
    from google.auth import default
    from google.auth.transport.requests import Request
    _HAS_GOOGLE_AUTH = True
except ImportError:
    _HAS_GOOGLE_AUTH = False


class GeminiClient:
    """Unified client for Google Gemini API via Vertex AI.

    1. Text/analysis (image-to-text) using gemini-2.5-pro
    2. Image generation (image-to-image) using gemini-2.5-flash-image
    """

    MODEL_TEXT = "gemini-2.5-pro"
    MODEL_IMAGE = "gemini-2.5-flash-image"

    ASPECT_RATIOS = {
        "1:1": (1024, 1024),
        "2:3": (832, 1248),
        "3:2": (1248, 832),
        "3:4": (864, 1184),
        "4:3": (1184, 864),
        "4:5": (896, 1152),
        "5:4": (1152, 896),
        "9:16": (768, 1344),
        "16:9": (1344, 768),
        "21:9": (1536, 672),
    }

    def __init__(
        self,
        project: Optional[str] = None,
        location: Optional[str] = None,
    ) -> None:
        self.project = project or os.getenv("GOOGLE_CLOUD_PROJECT", "tensile-tenure-473923-u8")
        self.location = location or os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
        self.model = self.MODEL_TEXT
        self._credentials = None
        self._auth_request = Request() if _HAS_GOOGLE_AUTH else None
        self._setup_credentials()

    def _setup_credentials(self) -> None:
        if not _HAS_GOOGLE_AUTH:
            return
        existing = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if existing and os.path.exists(existing):
            return
        for path in [
            "keys.json",
            "/workspace/keys/service-account-key.json",
            "/mnt/trescomas/keys/service-account-key.json",
        ]:
            if os.path.exists(path):
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
                return

    def get_access_token(self) -> str:
        if not _HAS_GOOGLE_AUTH:
            raise RuntimeError("google-auth is required for Gemini; pip install google-auth")
        if self._credentials is None:
            self._credentials, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        if not self._credentials.valid:
            self._credentials.refresh(self._auth_request)
        return self._credentials.token

    def _encode_image_to_base64(self, image_path: str) -> str:
        with open(image_path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")

    def _encode_image_bytes_to_base64(self, image_bytes: bytes) -> str:
        return base64.b64encode(image_bytes).decode("utf-8")

    def _get_mime_type(self, image_path: str) -> str:
        ext = os.path.splitext(image_path)[1].lower()
        return {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".gif": "image/gif", ".webp": "image/webp"}.get(ext, "image/jpeg")

    def _normalize_images(
        self,
        images: Union[str, List[str], bytes, List[bytes], None],
    ) -> List[dict]:
        if not images:
            return []
        image_list = [images] if isinstance(images, (str, bytes)) else images
        parts = []
        for item in image_list:
            if isinstance(item, str):
                if not os.path.exists(item):
                    raise FileNotFoundError(f"Image not found: {item}")
                data_b64 = self._encode_image_to_base64(item)
                mime = self._get_mime_type(item)
            else:
                data_b64 = self._encode_image_bytes_to_base64(item)
                mime = "image/png"
            parts.append({"inline_data": {"mime_type": mime, "data": data_b64}})
        return parts

    def _build_api_url(self, model: Optional[str] = None) -> str:
        m = model or self.model
        return (
            f"https://{self.location}-aiplatform.googleapis.com/v1/projects/"
            f"{self.project}/locations/{self.location}/publishers/google/"
            f"models/{m}:generateContent"
        )

    # ---- Text / analysis (image-to-text) ----

    def analyze_image(
        self,
        prompt: str,
        temperature: float = 0.7,
        images: Union[str, List[str], bytes, List[bytes], None] = None,
        response_mime_type: Optional[str] = None,
    ) -> Union[str, bytes]:
        """Image-to-text: analyze image(s) and return text or JSON."""
        url = self._build_api_url(self.MODEL_TEXT)
        headers = {
            "Authorization": f"Bearer {self.get_access_token()}",
            "Content-Type": "application/json; charset=utf-8",
        }
        parts = [{"text": prompt}]
        parts.extend(self._normalize_images(images))
        generation_config: dict = {"temperature": temperature}
        if response_mime_type:
            generation_config["response_mime_type"] = response_mime_type
        payload = {
            "contents": [{"role": "user", "parts": parts}],
            "generationConfig": generation_config,
        }
        resp = requests.post(url, headers=headers, data=json.dumps(payload))
        if resp.status_code != 200:
            raise RuntimeError(f"Vertex AI error {resp.status_code}: {resp.text}")
        return self._parse_text_response(resp.json())

    def extract_json_from_response(self, response: str) -> dict:
        """Extract JSON from Gemini text (handles ```json ... ``` and raw JSON)."""
        try:
            m = re.search(r"```json\s*(\{.*?\})\s*```", response, re.DOTALL)
            if m:
                json_str = m.group(1)
            else:
                m = re.search(r"\{.*\}", response, re.DOTALL)
                json_str = m.group(0) if m else response
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                fixed = re.sub(r"(\n\s*)([a-zA-Z_][a-zA-Z0-9_]*)\s*:", r'\1"\2":', json_str)
                return json.loads(fixed)
        except (json.JSONDecodeError, AttributeError) as e:
            return {"meta": {"note": "Raw response (not JSON)", "error": str(e)}, "raw_response": response}

    def _parse_text_response(self, data: dict) -> Union[str, bytes]:
        parts = data["candidates"][0]["content"]["parts"]
        for part in parts:
            if "inline_data" in part and (part["inline_data"].get("mime_type") or "").startswith("image/"):
                return base64.b64decode(part["inline_data"]["data"])
        return (parts[0].get("text") or "")

    # ---- Image generation (image-to-image) ----

    def generate_image(
        self,
        prompt: str,
        image_input: Optional[Union[str, bytes, List[str], List[bytes]]] = None,
        aspect_ratio: str = "1:1",
        temperature: float = 0.7,
    ) -> bytes:
        """Image-to-image: generate image with gemini-2.5-flash-image."""
        if aspect_ratio not in self.ASPECT_RATIOS:
            raise ValueError(f"Unsupported aspect_ratio: {aspect_ratio}. Supported: {list(self.ASPECT_RATIOS.keys())}")
        url = self._build_api_url(self.MODEL_IMAGE)
        headers = {
            "Authorization": f"Bearer {self.get_access_token()}",
            "Content-Type": "application/json; charset=utf-8",
        }
        parts = [{"text": prompt}]
        parts.extend(self._normalize_images(image_input))
        payload = {
            "contents": [{"role": "user", "parts": parts}],
            "generationConfig": {
                "responseModalities": ["IMAGE"],
                "imageConfig": {"aspectRatio": aspect_ratio},
                "temperature": temperature,
            },
        }
        resp = requests.post(url, headers=headers, data=json.dumps(payload))
        if resp.status_code != 200:
            raise RuntimeError(f"Gemini API error {resp.status_code}: {resp.text}")
        return self._parse_image_response(resp.json())

    def _parse_image_response(self, data: dict) -> bytes:
        candidates = data.get("candidates", [])
        if not candidates:
            raise RuntimeError(f"No candidates: {json.dumps(data, indent=2)}")
        parts = candidates[0].get("content", {}).get("parts", [])
        for part in parts:
            inline = part.get("inline_data") or part.get("inlineData")
            if inline:
                mime = inline.get("mime_type") or inline.get("mimeType", "")
                if mime.startswith("image/"):
                    return base64.b64decode(inline.get("data", ""))
        raise RuntimeError(f"No image in response. Parts: {[list(p.keys()) for p in parts]}")
