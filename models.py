from sqlalchemy import create_engine
from sqlalchemy import Column,String,Text,Integer,DateTime
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import sessionmaker

Base = declarative_base()


def db_connect():
    """
    Performs database connection using database settings from settings.py.
    Returns sqlalchemy engine instance
    """
    uri = 'mysql+pymysql://root:jinzheng1706@139.198.191.224:3308/news_oil'
    # print('uri',get_project_settings().get("SQL_CONNECT_STRING"))
    return create_engine(uri)



class OeNewsPro(Base):
    __tablename__ = 'news_oil_oe_pro'

    id = Column(Integer, primary_key=True)
    orig_id = Column(Integer)
    source = Column(String(255))
    title = Column(String(255))
    abstracts = Column(Text)
    preview_img_link = Column(String(255))
    url = Column(String(1024))
    format_pub_time = Column(DateTime)
    author = Column(String(255))
    new_content = Column(Text)
    categories = Column(String(255))
    img_urls_new = Column(String(5024))
    format_crawl_time = Column(DateTime)
    regions_merged = Column(String(255))
    country_merged = Column(String(255))
    company_keyword = Column(String(1024))
    country_matched_by_company_merged = Column(String(255))
    subcategory_merged = Column(String(255))
    topic_merged = Column(String(255))
    field_keyword = Column(String(255))
    storage_keyword = Column(String(255))
    mark_note_by_url = Column(String(32))


class WorldOilPro(Base):
    __tablename__ = 'world_oil_pro'

    id = Column(Integer, primary_key=True)
    orig_id = Column(Integer)
    source = Column(String(255))
    title = Column(String(255))
    abstracts = Column(Text)
    preview_img_link = Column(String(255))
    url = Column(String(1024))
    format_pub_time = Column(DateTime)
    author = Column(String(255))
    new_content = Column(Text)
    categories = Column(String(255))
    img_urls_new = Column(String(5024))
    format_crawl_time = Column(DateTime)
    regions_merged = Column(String(255))
    country_merged = Column(String(255))
    company_keyword = Column(String(1024))
    country_matched_by_company_merged = Column(String(255))
    subcategory_merged = Column(String(255))
    topic_merged = Column(String(255))
    field_keyword = Column(String(255))
    storage_keyword = Column(String(255))
    mark_note_by_url = Column(String(32))

class CnpcNewsPro(Base):
    __tablename__ = 'cnpc_news_pro'

    id = Column(Integer, primary_key=True)
    orig_id = Column(Integer)
    source = Column(String(255))
    title = Column(String(255))
    abstracts = Column(Text)
    preview_img_link = Column(String(255))
    url = Column(String(1024))
    format_pub_time = Column(DateTime)
    author = Column(String(255))
    new_content = Column(Text)
    categories = Column(String(255))
    img_urls_new = Column(String(5024))
    format_crawl_time = Column(DateTime)
    regions_merged = Column(String(255))
    country_merged = Column(String(255))
    company_keyword = Column(String(1024))
    country_matched_by_company_merged = Column(String(255))
    subcategory_merged = Column(String(255))
    topic_merged = Column(String(255))
    field_keyword = Column(String(255))
    storage_keyword = Column(String(255))
    mark_note_by_url = Column(String(32))


class HartEnergyPro(Base):
    __tablename__ = 'hart_energy_pro'

    id = Column(Integer, primary_key=True)
    orig_id = Column(Integer)
    source = Column(String(255))
    title = Column(String(255))
    abstracts = Column(Text)
    preview_img_link = Column(String(255))
    url = Column(String(1024))
    format_pub_time = Column(DateTime)
    author = Column(String(255))
    new_content = Column(Text)
    categories = Column(String(255))
    img_urls_new = Column(String(5024))
    format_crawl_time = Column(DateTime)
    regions_merged = Column(String(255))
    country_merged = Column(String(255))
    company_keyword = Column(String(1024))
    country_matched_by_company_merged = Column(String(255))
    subcategory_merged = Column(String(255))
    topic_merged = Column(String(255))
    field_keyword = Column(String(255))
    storage_keyword = Column(String(255))
    mark_note_by_url = Column(String(32))

class OilFieldTech(Base):
    __tablename__ = 'oilfield_tech_pro'

    id = Column(Integer, primary_key=True)
    orig_id = Column(Integer)
    source = Column(String(255))
    title = Column(String(255))
    abstracts = Column(Text)
    preview_img_link = Column(String(255))
    url = Column(String(1024))
    format_pub_time = Column(DateTime)
    author = Column(String(255))
    new_content = Column(Text)
    categories = Column(String(255))
    img_urls_new = Column(String(5024))
    format_crawl_time = Column(DateTime)
    regions_merged = Column(String(255))
    country_merged = Column(String(255))
    company_keyword = Column(String(1024))
    country_matched_by_company_merged = Column(String(255))
    subcategory_merged = Column(String(255))
    topic_merged = Column(String(255))
    field_keyword = Column(String(255))
    storage_keyword = Column(String(255))
    mark_note_by_url = Column(String(32))

class OilAndGas(Base):
    __tablename__ = 'oil_and_gas_pro'

    id = Column(Integer, primary_key=True)
    orig_id = Column(Integer)
    source = Column(String(255))
    title = Column(String(255))
    abstracts = Column(Text)
    preview_img_link = Column(String(255))
    url = Column(String(1024))
    format_pub_time = Column(DateTime)
    author = Column(String(255))
    new_content = Column(Text)
    categories = Column(String(255))
    img_urls_new = Column(String(5024))
    format_crawl_time = Column(DateTime)
    regions_merged = Column(String(255))
    country_merged = Column(String(255))
    company_keyword = Column(String(1024))
    country_matched_by_company_merged = Column(String(255))
    subcategory_merged = Column(String(255))
    topic_merged = Column(String(255))
    field_keyword = Column(String(255))
    storage_keyword = Column(String(255))
    mark_note_by_url = Column(String(32))


class InEnStorage(Base):
    __tablename__ = 'in_en_storage_pro'

    id = Column(Integer, primary_key=True)
    orig_id = Column(Integer)
    source = Column(String(255))
    title = Column(String(255))
    abstracts = Column(Text)
    preview_img_link = Column(String(255))
    url = Column(String(1024))
    format_pub_time = Column(DateTime)
    author = Column(String(255))
    new_content = Column(Text)
    categories = Column(String(255))
    img_urls_new = Column(String(5024))
    format_crawl_time = Column(DateTime)
    regions_merged = Column(String(255))
    country_merged = Column(String(255))
    company_keyword = Column(String(1024))
    country_matched_by_company_merged = Column(String(255))
    subcategory_merged = Column(String(255))
    topic_merged = Column(String(255))
    field_keyword = Column(String(255))
    storage_keyword = Column(String(255))
    mark_note_by_url = Column(String(32))

class JptLatestPro(Base):
    __tablename__ = 'jpt_latest_pro'

    id = Column(Integer, primary_key=True)
    orig_id = Column(Integer)
    source = Column(String(255))
    title = Column(String(255))
    abstracts = Column(Text)
    preview_img_link = Column(String(255))
    url = Column(String(1024))
    format_pub_time = Column(DateTime)
    author = Column(String(255))
    new_content = Column(Text)
    categories = Column(String(255))
    img_urls_new = Column(String(5024))
    format_crawl_time = Column(DateTime)
    regions_merged = Column(String(255))
    country_merged = Column(String(255))
    company_keyword = Column(String(1024))
    country_matched_by_company_merged = Column(String(255))
    subcategory_merged = Column(String(255))
    topic_merged = Column(String(255))
    field_keyword = Column(String(255))
    storage_keyword = Column(String(255))
    mark_note_by_url = Column(String(32))


class TempTable(Base):
    __tablename__ = 'temp_table'

    id = Column(Integer, primary_key=True)
    orig_id = Column(Integer)
    source = Column(String(255))
    title = Column(String(255))
    abstracts = Column(Text)
    preview_img_link = Column(String(255))
    url = Column(String(1024))
    format_pub_time = Column(DateTime)
    author = Column(String(255))
    new_content = Column(Text)
    categories = Column(String(255))
    img_urls_new = Column(String(5024))
    format_crawl_time = Column(DateTime)
    regions_merged = Column(String(255))
    country_merged = Column(String(255))
    company_keyword = Column(String(1024))
    country_matched_by_company_merged = Column(String(255))
    subcategory_merged = Column(String(255))
    topic_merged = Column(String(255))
    field_keyword = Column(String(255))
    storage_keyword = Column(String(255))
    mark_note_by_url = Column(String(32))


    
def create_table(engine):
    Base.metadata.create_all(engine)