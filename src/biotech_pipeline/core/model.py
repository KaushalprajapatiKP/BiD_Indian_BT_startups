"""
ORM model definitions for BiD_Indian_BT_startups PostgreSQL schema.
"""

from datetime import datetime
from sqlalchemy import (
    Column, String, Text, Integer, SmallInteger, Date,
    Numeric, Boolean, TIMESTAMP, ForeignKey
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()
TZ_NOW = lambda: datetime.utcnow()


class Company(Base):
    __tablename__ = "companies"

    big_award_id       = Column(String(50), primary_key=True, unique=True, nullable=False, doc="BIRAC BIG award identifier")
    registered_name    = Column(String(255), nullable=False, doc="Official registered company name")
    original_awardee   = Column(String(255), doc="Name of individual awardee if different")
    big_award_year     = Column(SmallInteger, doc="Year when the award was granted")
    website_url        = Column(Text, doc="Official company website URL")
    cin                = Column(String(50), unique=True, doc="Company Identification Number from MCA")
    incorporation_date = Column(Date, doc="Date of incorporation as per MCA")
    location           = Column(Text, doc="Registered address or headquarters location")
    mca_status         = Column(String(50), doc="Company legal status (Active/Dormant/etc.)")
    data_quality_score = Column(Numeric(3,2), default=0, doc="Data quality score (0.00–1.00)")
    created_at         = Column(TIMESTAMP(timezone=True), default=TZ_NOW)
    updated_at         = Column(TIMESTAMP(timezone=True), default=TZ_NOW, onupdate=TZ_NOW)

    people             = relationship("Person",          back_populates="company", cascade="all, delete-orphan")
    patents            = relationship("Patent",          back_populates="company", cascade="all, delete-orphan")
    publications       = relationship("Publication",     back_populates="company", cascade="all, delete-orphan")
    products_services  = relationship("ProductService",  back_populates="company", cascade="all, delete-orphan")
    funding_rounds     = relationship("FundingRound",    back_populates="company", cascade="all, delete-orphan")
    news               = relationship("NewsCoverage",    back_populates="company", cascade="all, delete-orphan")
    extraction_logs    = relationship("ExtractionLog",   back_populates="company", cascade="all, delete-orphan")


class Person(Base):
    __tablename__ = "people"

    person_id     = Column(Integer, primary_key=True, autoincrement=True)
    big_award_id  = Column(String(50), ForeignKey("companies.big_award_id", ondelete="CASCADE"))
    full_name     = Column(String(255), nullable=False, doc="Person’s full name")
    designation   = Column(String(255), doc="Role/designation")
    role_type     = Column(String(50), doc="Core Team, Advisor, Founder, Board Member")
    source        = Column(String(100), doc="Data source (website, pdf, etc.)")
    source_url    = Column(Text, doc="Source URL")
    created_at    = Column(TIMESTAMP(timezone=True), default=TZ_NOW)

    company       = relationship("Company", back_populates="people")


class Patent(Base):
    __tablename__ = "patents"

    patent_id            = Column(Integer, primary_key=True, autoincrement=True)
    big_award_id         = Column(String(50), ForeignKey("companies.big_award_id", ondelete="CASCADE"))
    patent_number        = Column(String(100), unique=True, nullable=False, doc="Patent number (PCT/WIPO/Indian/etc.)")
    patent_type          = Column(String(100), doc="Patent type/category")
    title                = Column(Text, doc="Patent title")
    inventors            = Column(Text, doc="Semicolon-separated list of inventors")
    filing_year          = Column(SmallInteger, doc="Year of filing")
    indian_jurisdiction  = Column(Boolean, default=False, doc="Is Indian jurisdiction?")
    foreign_jurisdiction = Column(Boolean, default=False, doc="Mentions any foreign jurisdiction?")
    jurisdiction_list    = Column(Text, doc="Comma-separated list of jurisdictions")
    source               = Column(String(100), doc="Data source (patentscope, espacenet, etc.)")
    source_url           = Column(Text, doc="Source URL")
    created_at           = Column(TIMESTAMP(timezone=True), default=TZ_NOW)

    company              = relationship("Company", back_populates="patents")


class Publication(Base):
    __tablename__ = "publications"

    publication_id   = Column(Integer, primary_key=True, autoincrement=True)
    big_award_id     = Column(String(50), ForeignKey("companies.big_award_id", ondelete="CASCADE"))
    pubmed_id        = Column(Integer, unique=True, doc="PubMed ID")
    title            = Column(Text, doc="Publication title")
    journal          = Column(String(255), doc="Journal name")
    publication_year = Column(SmallInteger, doc="Year of publication")
    citation_text    = Column(Text, doc="Formatted citation")
    source           = Column(String(100), doc="Data source (pubmed, website)")
    source_url       = Column(Text, doc="Source URL")
    created_at       = Column(TIMESTAMP(timezone=True), default=TZ_NOW)

    company          = relationship("Company", back_populates="publications")


class ProductService(Base):
    __tablename__ = "products_services"

    product_id        = Column(Integer, primary_key=True, autoincrement=True)
    big_award_id      = Column(String(50), ForeignKey("companies.big_award_id", ondelete="CASCADE"))
    product_name      = Column(String(255), doc="Product or service name")
    development_stage = Column(String(100), doc="Research, Clinical, Commercial")
    source            = Column(String(100), doc="Data source")
    source_url        = Column(Text, doc="Source URL")
    created_at        = Column(TIMESTAMP(timezone=True), default=TZ_NOW)

    company           = relationship("Company", back_populates="products_services")


class FundingRound(Base):
    __tablename__ = "funding_rounds"

    funding_id     = Column(Integer, primary_key=True, autoincrement=True)
    big_award_id   = Column(String(50), ForeignKey("companies.big_award_id", ondelete="CASCADE"))
    stage          = Column(String(20), doc="Stage of funding: pre-seed, seed, Series A…")
    amount_inr     = Column(Numeric(18,2), doc="Amount of funding")
    source_name    = Column(Text, doc="Name of funder")
    source_type    = Column(String(20), doc="Type of funder: government, VC, bank, other")
    funding_type   = Column(String(10), doc="Type of funding: grant, equity, debt")
    announced_date = Column(Date, doc="Date when round was announced")
    data_source    = Column(String(100), doc="Data source (news, API)")
    source_url     = Column(Text, doc="Source URL")
    created_at     = Column(TIMESTAMP(timezone=True), default=TZ_NOW)
    updated_at     = Column(TIMESTAMP(timezone=True), default=TZ_NOW, onupdate=TZ_NOW)

    company        = relationship("Company", back_populates="funding_rounds")


class NewsCoverage(Base):
    __tablename__ = "news_coverage"

    news_id        = Column(Integer, primary_key=True, autoincrement=True)
    big_award_id   = Column(String(50), ForeignKey("companies.big_award_id", ondelete="CASCADE"))
    headline       = Column(Text, doc="News headline")
    published_date = Column(Date, doc="Date published")
    news_category  = Column(String(100), doc="Funding, Product, Partnership, etc.")
    article_url    = Column(Text, doc="URL of the article")
    scraped_at     = Column(TIMESTAMP(timezone=True), default=TZ_NOW)

    company        = relationship("Company", back_populates="news")


class ExtractionLog(Base):
    __tablename__ = "extraction_log"

    log_id            = Column(Integer, primary_key=True, autoincrement=True)
    big_award_id      = Column(String(50), ForeignKey("companies.big_award_id", ondelete="CASCADE"))
    data_type         = Column(String(100), doc="Type of data collected")
    extraction_status = Column(String(50), doc="Success, Failed, Partial")
    records_found     = Column(Integer, default=0, doc="Number of records found")
    error_message     = Column(Text, doc="Error message if any")
    source_url        = Column(Text, doc="URL source of data")
    extracted_at      = Column(TIMESTAMP(timezone=True), default=TZ_NOW)

    company           = relationship("Company", back_populates="extraction_logs")
