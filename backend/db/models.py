from datetime import datetime
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .config import Base


SourceEnum = Enum("stripe", "hotmart", "kajabi", name="source_enum")


class Customer(Base):
    __tablename__ = "customers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(SourceEnum, nullable=False)
    source_id: Mapped[str] = mapped_column(String(255), nullable=False)
    email: Mapped[Optional[str]] = mapped_column(String(320))
    country: Mapped[Optional[str]] = mapped_column(String(2))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("source", "source_id", name="uq_customer_source_id"),
    )


class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(SourceEnum, nullable=False)
    source_id: Mapped[str] = mapped_column(String(255), nullable=False)
    name: Mapped[Optional[str]] = mapped_column(String(255))
    sku: Mapped[Optional[str]] = mapped_column(String(255))
    currency_default: Mapped[Optional[str]] = mapped_column(String(3))
    price_standard: Mapped[Optional[float]] = mapped_column(Numeric(18, 2))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("source", "source_id", name="uq_product_source_id"),
    )


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(SourceEnum, nullable=False)
    source_id: Mapped[str] = mapped_column(String(255), nullable=False)
    customer_id: Mapped[Optional[int]] = mapped_column(ForeignKey("customers.id"))
    status: Mapped[Optional[str]] = mapped_column(String(50))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    customer: Mapped[Optional[Customer]] = relationship("Customer")

    __table_args__ = (
        UniqueConstraint("source", "source_id", name="uq_order_source_id"),
        Index("ix_orders_created_at", "created_at"),
    )


class OrderItem(Base):
    __tablename__ = "order_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"), nullable=False)
    product_id: Mapped[Optional[int]] = mapped_column(ForeignKey("products.id"))
    quantity: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    unit_price_original_minor: Mapped[Optional[int]] = mapped_column(Integer)
    currency_original: Mapped[Optional[str]] = mapped_column(String(3))
    unit_price_eur: Mapped[Optional[float]] = mapped_column(Numeric(18, 2))

    order: Mapped[Order] = relationship("Order")
    product: Mapped[Optional[Product]] = relationship("Product")


class Payment(Base):
    __tablename__ = "payments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[Optional[int]] = mapped_column(ForeignKey("orders.id"))
    source: Mapped[str] = mapped_column(SourceEnum, nullable=False)
    source_payment_id: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False)
    amount_original_minor: Mapped[int] = mapped_column(Integer, nullable=False)
    currency_original: Mapped[str] = mapped_column(String(3), nullable=False)
    amount_eur: Mapped[Optional[float]] = mapped_column(Numeric(18, 2))
    fee_eur: Mapped[Optional[float]] = mapped_column(Numeric(18, 2))
    net_eur: Mapped[Optional[float]] = mapped_column(Numeric(18, 2))
    paid_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    raw: Mapped[Optional[dict]] = mapped_column(JSONB)

    order: Mapped[Optional[Order]] = relationship("Order")

    __table_args__ = (
        UniqueConstraint("source", "source_payment_id", name="uq_payment_source_id"),
        Index("ix_payments_paid_at", "paid_at"),
    )


class Refund(Base):
    __tablename__ = "refunds"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    payment_id: Mapped[int] = mapped_column(ForeignKey("payments.id"), nullable=False)
    amount_original_minor: Mapped[int] = mapped_column(Integer, nullable=False)
    currency_original: Mapped[str] = mapped_column(String(3), nullable=False)
    amount_eur: Mapped[Optional[float]] = mapped_column(Numeric(18, 2))
    reason: Mapped[Optional[str]] = mapped_column(String(255))
    refunded_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    raw: Mapped[Optional[dict]] = mapped_column(JSONB)

    payment: Mapped[Payment] = relationship("Payment")


class SyncState(Base):
    __tablename__ = "sync_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)



class Subscription(Base):
    __tablename__ = "subscriptions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(SourceEnum, nullable=False)
    source_id: Mapped[str] = mapped_column(String(255), nullable=False)
    customer_id: Mapped[Optional[int]] = mapped_column(ForeignKey("customers.id"))
    status: Mapped[Optional[str]] = mapped_column(String(50))
    interval: Mapped[Optional[str]] = mapped_column(String(20))
    amount_original_minor: Mapped[Optional[int]] = mapped_column(Integer)
    currency_original: Mapped[Optional[str]] = mapped_column(String(3))
    trial_ends_on: Mapped[Optional[datetime]] = mapped_column(DateTime)
    canceled_on: Mapped[Optional[datetime]] = mapped_column(DateTime)
    next_payment_date: Mapped[Optional[datetime]] = mapped_column(DateTime)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    customer: Mapped[Optional[Customer]] = relationship("Customer")

    __table_args__ = (
        UniqueConstraint("source", "source_id", name="uq_subscription_source_id"),
        Index("ix_subscriptions_next_payment", "next_payment_date"),
    )


# --- Ads / Analytics ---

class AdAccount(Base):
    __tablename__ = "ad_accounts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    platform: Mapped[str] = mapped_column(String(20), nullable=False)  # 'google_ads', 'meta'
    account_id: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[Optional[str]] = mapped_column(String(255))
    currency: Mapped[Optional[str]] = mapped_column(String(3))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("platform", "account_id", name="uq_ad_account_platform_id"),
    )


class AdCampaign(Base):
    __tablename__ = "ad_campaigns"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    platform: Mapped[str] = mapped_column(String(20), nullable=False)
    account_id: Mapped[str] = mapped_column(String(64), nullable=False)
    campaign_id: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[Optional[str]] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("platform", "campaign_id", name="uq_ad_campaign_platform_id"),
        Index("ix_ad_campaigns_account", "platform", "account_id"),
    )


class AdCostsDaily(Base):
    __tablename__ = "ad_costs_daily"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date: Mapped[datetime] = mapped_column(DateTime, index=True, nullable=False)
    platform: Mapped[str] = mapped_column(String(20), nullable=False)
    account_id: Mapped[Optional[str]] = mapped_column(String(64))
    campaign_id: Mapped[Optional[str]] = mapped_column(String(64))
    adset_id: Mapped[Optional[str]] = mapped_column(String(64))
    ad_id: Mapped[Optional[str]] = mapped_column(String(64))
    currency: Mapped[Optional[str]] = mapped_column(String(3))
    cost_major: Mapped[Optional[float]] = mapped_column(Numeric(18, 6))
    impressions: Mapped[Optional[int]] = mapped_column(Integer)
    clicks: Mapped[Optional[int]] = mapped_column(Integer)

    __table_args__ = (
        Index("ix_ad_costs_daily_dim", "platform", "account_id", "campaign_id", "date"),
    )


class AdAdset(Base):
    __tablename__ = "ad_adsets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    platform: Mapped[str] = mapped_column(String(20), nullable=False)
    adset_id: Mapped[str] = mapped_column(String(64), nullable=False)
    account_id: Mapped[Optional[str]] = mapped_column(String(64))
    name: Mapped[Optional[str]] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("platform", "adset_id", name="uq_adset_platform_id"),
        Index("ix_adsets_account", "platform", "account_id"),
    )


class AdAd(Base):
    __tablename__ = "ad_ads"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    platform: Mapped[str] = mapped_column(String(20), nullable=False)
    ad_id: Mapped[str] = mapped_column(String(64), nullable=False)
    account_id: Mapped[Optional[str]] = mapped_column(String(64))
    name: Mapped[Optional[str]] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("platform", "ad_id", name="uq_ad_platform_id"),
        Index("ix_ads_account", "platform", "account_id"),
    )


class GaSessionsDaily(Base):
    __tablename__ = "ga_sessions_daily"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date: Mapped[datetime] = mapped_column(DateTime, index=True, nullable=False)
    source: Mapped[Optional[str]] = mapped_column(String(100))
    medium: Mapped[Optional[str]] = mapped_column(String(100))
    campaign: Mapped[Optional[str]] = mapped_column(String(200))
    sessions: Mapped[Optional[int]] = mapped_column(Integer)
    users: Mapped[Optional[int]] = mapped_column(Integer)
    conversions: Mapped[Optional[int]] = mapped_column(Integer)

    __table_args__ = (
        Index("ix_ga_sessions_dim", "date", "source", "medium", "campaign"),
    )


class AttributionLink(Base):
    __tablename__ = "attribution_links"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    payment_id: Mapped[int] = mapped_column(ForeignKey("payments.id"), nullable=False)
    source: Mapped[Optional[str]] = mapped_column(String(100))
    medium: Mapped[Optional[str]] = mapped_column(String(100))
    campaign: Mapped[Optional[str]] = mapped_column(String(200))
    term: Mapped[Optional[str]] = mapped_column(String(200))
    content: Mapped[Optional[str]] = mapped_column(String(200))
    gclid: Mapped[Optional[str]] = mapped_column(String(255))
    fbclid: Mapped[Optional[str]] = mapped_column(String(255))
    weight: Mapped[Optional[float]] = mapped_column(Numeric(6, 4))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    payment: Mapped[Payment] = relationship("Payment")
    __table_args__ = (
        Index("ix_attrib_payment", "payment_id"),
    )


class AttributionEvent(Base):
    __tablename__ = "attribution_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    platform: Mapped[str] = mapped_column(String(20), nullable=False)  # 'ga4' | 'meta'
    event_name: Mapped[str] = mapped_column(String(100), nullable=False)
    event_time: Mapped[datetime] = mapped_column(DateTime, index=True, nullable=False)
    source: Mapped[Optional[str]] = mapped_column(String(100))
    medium: Mapped[Optional[str]] = mapped_column(String(100))
    campaign: Mapped[Optional[str]] = mapped_column(String(200))
    term: Mapped[Optional[str]] = mapped_column(String(200))
    content: Mapped[Optional[str]] = mapped_column(String(200))
    gclid: Mapped[Optional[str]] = mapped_column(String(255))
    fbclid: Mapped[Optional[str]] = mapped_column(String(255))
    transaction_id: Mapped[Optional[str]] = mapped_column(String(255))
    email: Mapped[Optional[str]] = mapped_column(String(320))
    value: Mapped[Optional[float]] = mapped_column(Numeric(18, 2))
    currency: Mapped[Optional[str]] = mapped_column(String(3))
    raw: Mapped[Optional[dict]] = mapped_column(JSONB)

    __table_args__ = (
        Index("ix_attrib_events_time", "event_time"),
        Index("ix_attrib_events_tx", "transaction_id"),
        Index("ix_attrib_events_gclid", "gclid"),
        Index("ix_attrib_events_fbclid", "fbclid"),
    )


# --- Leads y LTV ---

class LeadsKajabi(Base):
    __tablename__ = "leads_kajabi"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, index=True, nullable=False)
    email: Mapped[Optional[str]] = mapped_column(String(320))
    utm_source: Mapped[Optional[str]] = mapped_column(String(100))
    utm_medium: Mapped[Optional[str]] = mapped_column(String(100))
    utm_campaign: Mapped[Optional[str]] = mapped_column(String(200))
    utm_content: Mapped[Optional[str]] = mapped_column(String(200))
    gclid: Mapped[Optional[str]] = mapped_column(String(255))
    fbclid: Mapped[Optional[str]] = mapped_column(String(255))
    platform: Mapped[Optional[str]] = mapped_column(String(20))  # 'google_ads' | 'meta' | None
    campaign_id: Mapped[Optional[str]] = mapped_column(String(64))
    adset_id: Mapped[Optional[str]] = mapped_column(String(64))
    ad_id: Mapped[Optional[str]] = mapped_column(String(64))

    __table_args__ = (
        Index("ix_leads_kajabi_campaign", "utm_campaign"),
        Index("ix_leads_kajabi_platform", "platform"),
        Index("ix_leads_kajabi_gclid", "gclid"),
        Index("ix_leads_kajabi_fbclid", "fbclid"),
    )


class CustomerLTV(Base):
    __tablename__ = "customer_ltv"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    email: Mapped[str] = mapped_column(String(320), nullable=False)
    source: Mapped[str] = mapped_column(SourceEnum, nullable=False)
    ltv_eur: Mapped[float] = mapped_column(Numeric(18, 2), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("email", "source", name="uq_customer_ltv_email_source"),
        Index("ix_customer_ltv_email", "email"),
    )


class MetaInsightsDaily(Base):
    __tablename__ = "meta_insights_daily"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    account_id: Mapped[str] = mapped_column(String(64), nullable=False)
    campaign_id: Mapped[str] = mapped_column(String(64), nullable=False)
    adset_id: Mapped[str] = mapped_column(String(64), nullable=False)
    ad_id: Mapped[str] = mapped_column(String(64), nullable=False)
    purchases: Mapped[int] = mapped_column(Integer, default=0)
    purchase_value: Mapped[float] = mapped_column(Numeric(18, 4), default=0.0)
    currency: Mapped[str] = mapped_column(String(10))

    __table_args__ = (UniqueConstraint("date", "ad_id", name="uq_meta_insights_daily_date_ad"),)


class GoogleAdsInsightsDaily(Base):
    __tablename__ = "google_ads_insights_daily"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    account_id: Mapped[str] = mapped_column(String(64), nullable=False)
    campaign_id: Mapped[str] = mapped_column(String(64), nullable=False)
    adgroup_id: Mapped[str] = mapped_column(String(64), nullable=False) # Adset equivalent
    ad_id: Mapped[str] = mapped_column(String(64), nullable=False)
    conversions: Mapped[float] = mapped_column(Numeric(18, 4), default=0.0)
    conversions_value: Mapped[float] = mapped_column(Numeric(18, 4), default=0.0)
    currency: Mapped[str] = mapped_column(String(10))

    __table_args__ = (UniqueConstraint("date", "ad_id", name="uq_google_ads_insights_daily_date_ad"),)


class GA4PurchasesDaily(Base):
    """Purchases y revenue de GA4, fuente de verdad para revenue total.
    Segmentado por source/medium/campaign para poder atribuir a plataformas.
    """
    __tablename__ = "ga4_purchases_daily"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    source: Mapped[Optional[str]] = mapped_column(String(100))
    medium: Mapped[Optional[str]] = mapped_column(String(100))
    campaign: Mapped[Optional[str]] = mapped_column(String(200))
    item_name: Mapped[Optional[str]] = mapped_column(String(500))  # Opcional para granularidad
    purchases: Mapped[int] = mapped_column(Integer, default=0)
    revenue_eur: Mapped[float] = mapped_column(Numeric(18, 4), default=0.0)
    platform_detected: Mapped[Optional[str]] = mapped_column(String(20))  # 'google_ads', 'meta', 'organic', etc.
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        # Constraint único: date, source, medium, campaign, item_name (NULLs tratados como distintos)
        UniqueConstraint("date", "source", "medium", "campaign", "item_name", name="uq_ga4_purchases_daily"),
        Index("ix_ga4_purchases_date_platform", "date", "platform_detected"),
        Index("ix_ga4_purchases_date", "date"),
    )

