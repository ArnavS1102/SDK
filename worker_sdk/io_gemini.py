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
import time
import traceback
from typing import List, Optional, Union

import requests

from .logging import get_logger

_gemini_log = get_logger("io_gemini")

# Optional: google-auth for Vertex AI ADC
try:
    from google.auth import default
    from google.auth.transport.requests import Request
    _HAS_GOOGLE_AUTH = True
except ImportError:
    _HAS_GOOGLE_AUTH = False


def _bundled_vertex_wif_path() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "clientLibraryConfig-vastra-aws-provider.json")


def apply_bundled_vertex_wif_credentials() -> None:
    """If the bundled Workload Identity config exists, set GOOGLE_APPLICATION_CREDENTIALS to it."""
    path = _bundled_vertex_wif_path()
    if not os.path.isfile(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        _gemini_log.warning(
            "Could not read bundled Vertex WIF credential file",
            {"path": path, "error": str(e)},
        )
        return
    if data.get("type") != "external_account":
        _gemini_log.warning(
            "Bundled Vertex credential file is not Workload Identity (external_account); not forcing",
            {"path": path, "credential_type": data.get("type")},
        )
        return
    previous = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if previous != path:
        _gemini_log.info(
            "Using bundled Workload Identity config for Vertex (overriding GOOGLE_APPLICATION_CREDENTIALS)",
            {"previous": previous, "using": path},
        )
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path


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
        apply_bundled_vertex_wif_credentials()
        self.project = project or os.getenv("GOOGLE_CLOUD_PROJECT")
        if not self.project:
            raise RuntimeError(
                "GOOGLE_CLOUD_PROJECT env var is required. "
                "Set it to your GCP project ID (e.g. export GOOGLE_CLOUD_PROJECT=my-project-id)."
            )
        self.location = location or os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
        self.model = self.MODEL_TEXT
        self._credentials = None
        self._auth_request = Request() if _HAS_GOOGLE_AUTH else None
        self._setup_credentials()

    def _setup_credentials(self) -> None:
        if not _HAS_GOOGLE_AUTH:
            return
        creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if creds_path and not os.path.exists(creds_path):
            raise RuntimeError(
                f"GOOGLE_APPLICATION_CREDENTIALS is set to '{creds_path}' but the file does not exist. "
                "Mount the service account key file at that path before starting the worker."
            )
        # If GOOGLE_APPLICATION_CREDENTIALS is not set, google.auth.default() will fall through
        # to the standard ADC chain (metadata server, well-known file, etc.).

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
        t0 = time.time()
        image_bytes_total = 0
        image_count = 0
        if isinstance(images, bytes):
            image_bytes_total = len(images)
            image_count = 1
        elif isinstance(images, list):
            for x in images:
                if isinstance(x, bytes):
                    image_bytes_total += len(x)
                    image_count += 1
        elif images is not None:
            image_count = 1

        url = self._build_api_url(self.MODEL_TEXT)
        _gemini_log.info(
            "Gemini generateContent request (analyze_image)",
            {
                "model": self.MODEL_TEXT,
                "project": self.project,
                "location": self.location,
                "prompt_chars": len(prompt),
                "temperature": temperature,
                "image_count": image_count,
                "image_payload_bytes": image_bytes_total,
                "response_mime_type": response_mime_type,
            },
        )
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
        try:
            resp = requests.post(url, headers=headers, data=json.dumps(payload))
        except requests.RequestException as e:
            elapsed = round(time.time() - t0, 3)
            _gemini_log.error(
                "Gemini generateContent network error",
                {"error": str(e), "error_type": type(e).__name__, "elapsed_sec": elapsed, "traceback": traceback.format_exc()},
            )
            raise
        elapsed = round(time.time() - t0, 3)
        if resp.status_code != 200:
            _gemini_log.error(
                "Gemini generateContent HTTP error",
                {
                    "status_code": resp.status_code,
                    "elapsed_sec": elapsed,
                    "body_preview": (resp.text or "")[:2000],
                },
            )
            raise RuntimeError(f"Vertex AI error {resp.status_code}: {resp.text}")
        try:
            data = resp.json()
        except json.JSONDecodeError as e:
            _gemini_log.error(
                "Gemini generateContent invalid JSON body",
                {"elapsed_sec": elapsed, "error": str(e), "body_preview": (resp.text or "")[:500]},
            )
            raise
        try:
            result = self._parse_text_response(data)
        except Exception as e:
            _gemini_log.error(
                "Gemini generateContent parse response failed",
                {
                    "elapsed_sec": elapsed,
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "raw_keys": list(data.keys()) if isinstance(data, dict) else None,
                    "traceback": traceback.format_exc(),
                },
            )
            raise
        elapsed = round(time.time() - t0, 3)
        cand_n = len(data.get("candidates") or [])
        finish = None
        if cand_n:
            c0 = (data.get("candidates") or [None])[0] or {}
            finish = c0.get("finishReason") or c0.get("finish_reason")
        extra_ok: dict = {
            "model": self.MODEL_TEXT,
            "elapsed_sec": elapsed,
            "candidates_count": cand_n,
            "finish_reason": finish,
        }
        if isinstance(result, str):
            extra_ok["response_chars"] = len(result)
            extra_ok["response_preview"] = result[:160].replace("\n", " ") if result else ""
        else:
            extra_ok["response_bytes"] = len(result)
        _gemini_log.info("Gemini generateContent success (analyze_image)", extra_ok)
        return result

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
        """Extract assistant text; join all text parts (Gemini may split across parts)."""
        candidates = data.get("candidates") or []
        if not candidates:
            pf = data.get("promptFeedback") or data.get("prompt_feedback")
            raise RuntimeError(
                f"No Gemini candidates (blocked or empty). promptFeedback={pf!r} body_keys={list(data.keys())}"
            )
        cand = candidates[0]
        finish = cand.get("finishReason") or cand.get("finish_reason")
        content = cand.get("content") or {}
        parts = content.get("parts") or []
        if not parts:
            raise RuntimeError(
                f"No content parts in Gemini response; finishReason={finish!r} candidate_keys={list(cand.keys())}"
            )
        text_chunks: List[str] = []
        image_bytes: Optional[bytes] = None
        for part in parts:
            inline = part.get("inline_data") or part.get("inlineData")
            if inline:
                mime = (inline.get("mime_type") or inline.get("mimeType") or "")
                if mime.startswith("image/") and inline.get("data"):
                    image_bytes = base64.b64decode(inline["data"])
                continue
            t = part.get("text")
            if t:
                text_chunks.append(t)
        if text_chunks:
            return "".join(text_chunks).strip()
        if image_bytes is not None:
            return image_bytes
        raise RuntimeError(
            f"No text in Gemini response parts; finishReason={finish!r} part_keys={[list(p.keys()) for p in parts]}"
        )

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
