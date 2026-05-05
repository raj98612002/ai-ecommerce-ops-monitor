import json
import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")


def fallback_classify(text: str) -> dict:
    text_lower = text.lower()

    if "payment" in text_lower or "upi" in text_lower or "card" in text_lower:
        return {
            "category": "payment_issue",
            "severity": "high",
            "customer_emotion": "frustrated",
            "business_impact": "revenue_risk",
            "recommended_action": "Check payment gateway logs and failed payment events."
        }

    if "late" in text_lower or "delay" in text_lower or "delivery" in text_lower:
        return {
            "category": "late_delivery",
            "severity": "medium",
            "customer_emotion": "angry",
            "business_impact": "customer_churn_risk",
            "recommended_action": "Check delivery partner SLA and delayed delivery events."
        }

    if "refund" in text_lower:
        return {
            "category": "refund_delay",
            "severity": "high",
            "customer_emotion": "frustrated",
            "business_impact": "trust_risk",
            "recommended_action": "Check refund processing queue and payment reversal status."
        }

    if "wrong" in text_lower or "different" in text_lower:
        return {
            "category": "wrong_product",
            "severity": "medium",
            "customer_emotion": "disappointed",
            "business_impact": "return_risk",
            "recommended_action": "Check warehouse picking and product mapping process."
        }

    return {
        "category": "app_issue",
        "severity": "low",
        "customer_emotion": "confused",
        "business_impact": "minor_support_load",
        "recommended_action": "Check app logs and customer support ticket details."
    }


def classify_complaint(text: str) -> dict:
    api_key = os.getenv("OPENAI_API_KEY")

    if not api_key or api_key == "your_openai_key":
        return fallback_classify(text)

    client = OpenAI(api_key=api_key)

    system_prompt = """
You are an AI customer operations analyst for an e-commerce company.

Classify customer complaints into structured operational signals.

Return ONLY valid JSON.

Allowed categories:
- payment_issue
- late_delivery
- wrong_product
- refund_delay
- app_issue
- other

Allowed severity:
- low
- medium
- high
- critical

Allowed business_impact examples:
- revenue_risk
- customer_churn_risk
- trust_risk
- return_risk
- minor_support_load
- operational_risk

Do not add explanation outside JSON.
"""

    user_prompt = f"""
Complaint text:
{text}

Return JSON with this exact schema:
{{
  "category": "",
  "severity": "",
  "customer_emotion": "",
  "business_impact": "",
  "recommended_action": ""
}}
"""

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.1,
            response_format={"type": "json_object"},
        )

        result = json.loads(response.choices[0].message.content)

        return {
            "category": result.get("category", "other"),
            "severity": result.get("severity", "medium"),
            "customer_emotion": result.get("customer_emotion", "unknown"),
            "business_impact": result.get("business_impact", "operational_risk"),
            "recommended_action": result.get("recommended_action", "Review complaint manually.")
        }

    except Exception:
        return fallback_classify(text)