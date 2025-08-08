"""
Agentic AI Extraction using Llama.cpp
Free, open-source LLM with no API key required.
Extracts website URL and founders from a company name.
"""

import json
import re
from pathlib import Path
from llama_cpp import Llama

class SearchAgent:
    """
    Uses a local Llama.cpp model to generate structured JSON from prompts.
    """

    def __init__(self, model_path: str):
        """
        model_path: Path to ggml Llama model file (e.g., 'models/7B/ggml-model.bin')
        """
        if not Path(model_path).is_file():
            raise FileNotFoundError(f"Llama model not found at {model_path}")
        self.llm = Llama(model_path=model_path, n_ctx=2048)

    def find_website_and_founders(self, company_name: str) -> dict:
        """
        Prompt Llama to output a JSON object with keys:
          - website: official URL
          - founders: list of founder full names
        If unknown, fields should be empty.
        """
        prompt = f"""
You are a biotech research assistant. Given a company name, output JSON:
{{
  "website": "official website URL",
  "founders": ["Founder One","Founder Two"]
}}
If unknown, use empty string or empty list.
Company: "{company_name}"
###
"""
        resp = self.llm(prompt, max_tokens=200, stop=["###"])
        text = resp["choices"][0]["text"]
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            # Fallback: extract JSON substring
            match = re.search(r"\{.*\}", text, re.DOTALL)
            data = json.loads(match.group()) if match else {"website": "", "founders": []}
        # Ensure keys
        data.setdefault("website", "")
        data.setdefault("founders", [])
        return data
