from sqlalchemy import create_engine
from sqlalchemy import Column,String,Text,Integer
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import sessionmaker

Base = declarative_base()


def db_connect():
    """
    Performs database connection using database settings from settings.py.
    Returns sqlalchemy engine instance
    """
    uri = 'mysql+pymysql://root:password@localhost:3308/news_oil'
    # print('uri',get_project_settings().get("SQL_CONNECT_STRING"))
    return create_engine(uri)

def create_table(engine):
    Base.metadata.create_all(engine)

class OeNewsPro(Base):
    __tablename__ = 'news_oil_oe_pro'

    id = Column(Integer, primary_key=True)
    orig_id = Column(Integer)
    source = Column(String(255))
    title = Column(String(255))
    abstract = Column(String(255))
    preview_img_link = Column(String(255))
    url = Column(String(255))

    pub_time = Column(String(255))
    preview_img_link = Column(String(255))
    format_pub_time = Column(String(255))
    author = Column(String(255))
    new_content = Column(Text)
    img_urls_new = Column(String(255))
    ormat_crawl_time = Column(String(255))
    regions_merged = Column(String(255))
    country_merged = Column(String(255))
    company_keyword = Column(String(255))
    country_matched_by_company_merged = Column(String(255))
    subcategory_merged = Column(String(255))
    topic_merged = Column(String(255))
    field_keyword = Column(String(255))
    storage_keyword = Column(String(255))
    mark_note_by_url = Column(String(32))