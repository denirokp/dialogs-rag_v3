from typing import List, Dict
import openai
import os

class LLMClient:
    def __init__(self, **kwargs):
        self.client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = kwargs.get("model", "gpt-4o-mini")
        self.kwargs = kwargs

    def generate_json(self, system_prompt: str, user_prompt: str) -> List[Dict]:
        """Верните список словарей (JSON) по нашей схеме."""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            import json
            result = json.loads(response.choices[0].message.content)
            return result.get("mentions", []) if isinstance(result, dict) else result
        except Exception as e:
            print(f"LLM error: {e}")
            return []
