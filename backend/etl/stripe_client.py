import os
from datetime import datetime
from typing import Dict, Iterable, Optional

import stripe
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential


load_dotenv()
stripe.api_key = os.getenv("STRIPE_API_KEY", "")


def _to_unix(dt: datetime) -> int:
    return int(dt.timestamp())


@retry(wait=wait_exponential(multiplier=1, min=1, max=10), stop=stop_after_attempt(5))
def list_charges(updated_after: Optional[datetime] = None) -> Iterable[Dict]:
    params = {
        "limit": 100,
        "expand": ["data.balance_transaction"],
    }
    if updated_after:
        params["created"] = {"gte": _to_unix(updated_after)}

    starting_after = None
    while True:
        if starting_after:
            params["starting_after"] = starting_after
        page = stripe.Charge.list(**params)
        for ch in page["data"]:
            yield ch
        if not page["has_more"]:
            break
        starting_after = page["data"][-1]["id"]


@retry(wait=wait_exponential(multiplier=1, min=1, max=10), stop=stop_after_attempt(5))
def list_refunds(updated_after: Optional[datetime] = None) -> Iterable[Dict]:
    params = {
        "limit": 100,
    }
    if updated_after:
        params["created"] = {"gte": _to_unix(updated_after)}

    starting_after = None
    while True:
        if starting_after:
            params["starting_after"] = starting_after
        page = stripe.Refund.list(**params)
        for rf in page["data"]:
            yield rf
        if not page["has_more"]:
            break
        starting_after = page["data"][-1]["id"]


@retry(wait=wait_exponential(multiplier=1, min=1, max=10), stop=stop_after_attempt(5))
def list_subscriptions(updated_after: Optional[datetime] = None) -> Iterable[Dict]:
    params = {
        "limit": 100,
    }
    if updated_after:
        params["created"] = {"gte": _to_unix(updated_after)}

    starting_after = None
    while True:
        if starting_after:
            params["starting_after"] = starting_after
        page = stripe.Subscription.list(**params)
        for sub in page["data"]:
            yield sub
        if not page["has_more"]:
            break
        starting_after = page["data"][-1]["id"]


