import os
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

client = Groq(api_key=os.getenv("GROQ_API_KEY"))


def generate_campaign_copy(
    segment_name: str,
    segment_stats: dict,
    campaign_goal: str,
    keywords: str = "",
    product_links: list = None,
) -> dict:

    rfm_tiers   = segment_stats.get("rfm_dist", {})
    top_cats    = segment_stats.get("top_categories", {})
    email_optin = segment_stats.get("email_optin", 0)
    total       = segment_stats.get("total_customers", 0)
    avg_ltv     = segment_stats.get("avg_ltv", 0)

    # Build product links block
    links_block = ""
    if product_links:
        valid = [l.strip() for l in product_links if l.strip()]
        if valid:
            links_block = "\nProduct Links to include naturally in the email body:\n"
            for i, link in enumerate(valid, 1):
                links_block += f"  Product {i}: {link}\n"

    # Build keywords block
    keywords_block = ""
    if keywords and keywords.strip():
        keywords_block = f"\nAdditional keywords and tone guidance: {keywords.strip()}"

    prompt = f"""You are an expert email marketing copywriter for a retail brand.

Write a personalized email campaign for this customer segment:

Segment Name: {segment_name}
Campaign Goal: {campaign_goal}
Total Audience: {total:,} customers
Email Opted-In: {email_optin:,}
Avg LTV: ${avg_ltv:,.0f}
RFM Profile: {rfm_tiers}
Top Categories: {top_cats}{keywords_block}{links_block}

Instructions:
- Use the keywords naturally to shape tone and messaging
- If product links are provided, embed them as clickable anchor text in the body (e.g. <a href="URL">Shop Now</a>)
- Keep the email warm, personal and action-oriented
- Subject line max 60 characters
- Preview text max 90 characters
- Body: 3-4 short paragraphs

Respond ONLY in this exact format with no extra text or markdown:
SUBJECT: [subject line]
PREVIEW: [preview text]
BODY: [email body with HTML links if product links provided]
CTA: [button text, max 5 words]"""

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=1200,
        temperature=0.75,
    )

    raw = response.choices[0].message.content
    result = {"subject": "", "preview": "", "body": "", "cta": ""}

    if "BODY:" in raw and "CTA:" in raw:
        result["body"] = raw.split("BODY:")[1].split("CTA:")[0].strip()

    for line in raw.split("\n"):
        if line.startswith("SUBJECT:"):
            result["subject"] = line.replace("SUBJECT:", "").strip()
        elif line.startswith("PREVIEW:"):
            result["preview"] = line.replace("PREVIEW:", "").strip()
        elif line.startswith("CTA:"):
            result["cta"] = line.replace("CTA:", "").strip()

    return result