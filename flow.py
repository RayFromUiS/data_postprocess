import pandas as pd
import re
import time
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)
import datetime as dt
from datetime import datetime
from bs4 import BeautifulSoup
from models import db_connect, create_table, TempTable
from check_pro import return_no_processed_df
from utils import wash_process, wash_hart_energy_process, wash_world_oil, wash_oil_gas_process, wash_jpt_process \
    , wash_energy_voice, wash_gulf_oilgas, wash_energy_pedia, wash_upstream, wash_oil_price, wash_inen_tech, \
    upstream_preview_img, wash_drill_contractor, wash_natural_gas, wash_rig_zone, concate_img_content, \
    wash_offshore_tech, change_src_img, concate_offshore_img_content, wash_energy_year, wash_energy_china, \
    wash_china_five, wash_offshore_energy, wash_jwn_energy, wash_iran_oilgas, wash_nengyuan, \
    wash_woodmac, wash_rystadenergy, wash_westwood, wash_ieanews, upstream_main_img, extract_img_links, read_xlsx, \
    gen_keywords_pair, match_keyword, match_country_region, \
    chopoff, match_company, rematch_keywords, match_topic, match_storage, get_mark_urls, mark_title, add_same_key, \
    remove_intell_topic, mark_cnpc_hot, get_hart_energy_hot, get_world_oil_hot, get_oil_gas_hot, wash_weixin, \
    format_weixin_pubtime \
    , extract_weixin_img_links, pormat_cnpc_news_time, change_img_links

if __name__ == '__main__':

    start_time = time.time()
    table_name = ['news_oil_oe', 'world_oil', 'hart_energy', 'cnpc_news', 'oilfield_tech', \
                  'oil_and_gas', 'in_en_storage', 'jpt_latest', 'energy_voice', 'gulf_oil_gas', 'energy_pedia', \
                  'up_stream', 'oil_price', 'inen_tech', 'inen_newenergy', 'drill_contractor', 'rog_tech',\
                  'natural_gas', \
                  'rig_zone', 'offshore_tech', 'energy_year', 'energy_china', 'china_five', 'offshore_energy', \
                  'jwn_energy', 'iran_oil_gas', 'neng_yuan', 'wood_mac', 'rystad_energy', 'westwood_energy', 'iea_news',
                  'weixin_data', \
                  'weixin_oil_cross', 'weixin_lng_con', 'weixin_cnpc_news', 'weixin_energy_express',\
                  'weixin_petro_trading', \
                  'weixin_hai_bei', 'weixin_offshore_energy', 'weixin_hai_bo', 'weixin_crsl', 'weixin_oil_cubic',\
                  'weixin_oil_link']
    table_name_pro = ['news_oil_oe_pro', 'world_oil_pro', 'hart_energy_pro', 'cnpc_news_pro', 'oilfield_tech_pro', \
                      'oil_and_gas_pro', 'in_en_storage_pro', 'jpt_latest_pro', 'energy_voice_pro', 'gulf_oil_gas_pro', \
                      'energy_pedia_pro', 'up_stream_pro', 'oil_price_pro', 'inen_tech_pro', 'inen_newenergy_pro', \
                      'drill_contractor_pro', 'rog_tech_pro', 'natural_gas_pro', 'rig_zone_pro', 'offshore_tech_pro', \
                      'energy_year_pro', 'energy_china_pro', 'china_five_pro', 'offshore_energy_pro', 'jwn_energy_pro', \
                      'iran_oilgas_pro', 'neng_yuan_pro', 'wood_mac_pro', 'rystad_energy_pro', 'westwood_energy_pro',\
                      'iea_news_pro', 'weixin_data_pro', 'weixin_oil_cross_pro', 'weixin_lng_con_pro',\
                      'weixin_cnpc_news_pro', 'weixin_energy_express_pro', 'weixin_petro_trading_pro', \
                      'weixin_hai_bei_pro', 'weixin_offshore_energy_pro', 'weixin_hai_bo_pro', 'weixin_crsl_pro',\
                      'weixin_oil_cubic_pro', 'weixin_oil_link_pro']
    engine = db_connect()
    create_table(engine)
    cate_file = 'input_data/categories_list.xlsx'
    df_dicts = read_xlsx(cate_file)

    # ==================== generate all the keyword and category pair==================================
    # country section
    df_dicts['country'].columns = ['region', 'country', 'key_words_chinese', 'key_words_english']  ## rename cols
    country_keywords_pair = gen_keywords_pair(df_dicts['country'], 2, [3, 4])
    # region section
    df_dicts['region'].columns = ['region', 'chinese_keywords', 'english_keywords']
    region_df = df_dicts['region']
    region_df.columns = ['region', 'chinese_keywords', 'english_keywords']
    region_df['chinese_keywords'] = region_df['chinese_keywords'].apply(lambda x: x.split('，')[0])
    region_df['english_keywords'] = region_df['english_keywords'].apply(lambda x: x.split('、')[0])
    state_keywords_pair = gen_keywords_pair(region_df, 1, [2, 3])
    ##genreate the country-region dictionay data for adding the region data from country list
    countries = df_dicts['country']['country'].values
    regions = df_dicts['country']['region'].values
    country_region = {}
    for country, region in zip(countries, regions):
        country_region[country] = region
    ## company section
    df_company = df_dicts['company']
    df_company.columns = ['country', 'business', 'company', 'keywords']
    df_company['keywords'] = df_company['keywords']. \
        apply(lambda x: chopoff(x)). \
        apply(lambda x: x.strip()).apply(lambda x: x.strip().split('、'))
    company_keyword_pair = {}
    companies = df_company['company'].values
    keywords = df_company['keywords'].values
    for company, keyword in zip(companies, keywords):
        company_keyword_pair[company] = keyword
    # print(company_keyword)
    company_business = {}
    companies = df_company['company'].values
    business = df_company['business'].values
    for company, business in zip(companies, business):
        company_business[company] = business

    company_country = {}
    companies = df_company['company'].values
    counties = df_company['country'].values
    for company, country in zip(companies, counties):
        company_country[company] = country

    ## topic section

    df_dicts['subcategory'].columns = ['subset', 'topic', 'chinese_keywords', 'english_keywords']
    df_cate = df_dicts['subcategory']
    df_cate.iloc[28].topic = '其他'
    df_cate.dropna(inplace=True)
    df_cate['english_keywords'] = df_cate['english_keywords'].astype('str').apply(lambda x: x.split('、'))
    df_cate['chinese_keywords'] = df_cate['chinese_keywords'].astype('str').apply(lambda x: x.split('、'))

    ## rename all the chinese and english keywords
    geologies = df_cate.iloc[4].chinese_keywords
    smart_geology = df_cate.iloc[16].chinese_keywords
    drilling = df_cate.iloc[5].chinese_keywords
    smart_drilling = df_cate.iloc[17].chinese_keywords
    well_test = df_cate.iloc[6].chinese_keywords
    smart_test = df_cate.iloc[18].chinese_keywords
    production = df_cate.iloc[7].chinese_keywords
    smart_production = df_cate.iloc[19].chinese_keywords
    transport = df_cate.iloc[12].chinese_keywords + \
                df_cate.iloc[13].chinese_keywords + \
                df_cate.iloc[14].chinese_keywords + \
                df_cate.iloc[15].chinese_keywords
    smart_transport = df_cate.iloc[20].chinese_keywords

    geologies_english = df_cate.iloc[4].english_keywords
    smart_geology_english = df_cate.iloc[16].english_keywords
    drilling_english = df_cate.iloc[5].english_keywords
    smart_drilling_english = df_cate.iloc[17].english_keywords
    well_test_english = df_cate.iloc[6].english_keywords
    smart_test_english = df_cate.iloc[18].english_keywords
    production_english = df_cate.iloc[7].english_keywords
    smart_production_english = df_cate.iloc[19].english_keywords
    transport_english = df_cate.iloc[12].english_keywords + \
                        df_cate.iloc[13].english_keywords + \
                        df_cate.iloc[14].english_keywords + \
                        df_cate.iloc[15].english_keywords
    smart_transport_english = df_cate.iloc[20].english_keywords

    ##generate mixed keywords
    smart_geologies_chinese_mixed = rematch_keywords(geologies, smart_geology)
    smart_drill_chinese_mixed = rematch_keywords(drilling, smart_drilling)
    smart_well_test_chinese_mixed = rematch_keywords(well_test, smart_test)
    smart_production_chinese_mixed = rematch_keywords(production, smart_production)
    smart_transport_chinese_mixed = rematch_keywords(transport, smart_transport)
    smart_geologies_english_mixed = rematch_keywords(geologies_english, smart_geology_english)
    smart_drill_english_mixed = rematch_keywords(drilling_english, smart_drilling_english)
    smart_well_test_english_mixed = rematch_keywords(well_test_english, smart_test_english)
    smart_production_english_mixed = rematch_keywords(production_english, smart_production_english)
    smart_transport_english_mixed = rematch_keywords(transport_english, smart_transport_english)
    ## change the keywords with such mixed ones
    df_cate.iloc[16].chinese_keywords = smart_geologies_chinese_mixed
    df_cate.iloc[17].chinese_keywords = smart_drill_chinese_mixed
    df_cate.iloc[18].chinese_keywords = smart_well_test_chinese_mixed
    df_cate.iloc[19].chinese_keywords = smart_production_chinese_mixed
    df_cate.iloc[20].chinese_keywords = smart_transport_chinese_mixed
    df_cate.iloc[16].english_keywords = smart_geologies_english_mixed
    df_cate.iloc[17].english_keywords = smart_drill_english_mixed
    df_cate.iloc[18].english_keywords = smart_well_test_english_mixed
    df_cate.iloc[19].english_keywords = smart_production_english_mixed
    df_cate.iloc[20].english_keywords = smart_transport_english_mixed

    df_cate['keywords'] = df_cate['chinese_keywords'] + df_cate['english_keywords']
    ## preparing the category-keywords pair
    topic_keywords = {}
    topics = df_cate['topic'].values
    keywords = df_cate['keywords'].values
    for topic, keyword in zip(topics, keywords):
        topic_keywords[topic] = keyword
    topic_subcategory = {}

    topics = df_cate['topic'].values
    subcategory = df_cate['subset'].values
    for topic, keyword in zip(topics, subcategory):
        topic_subcategory[topic] = keyword
    topic_subcategory['石油公司'] = '能源公司'
    topic_subcategory['油服公司'] = '能源公司'
    ## field section
    df_dicts['field'].columns = ['field', 'keyword']
    df_field = df_dicts['field']
    df_field['merged_keywords'] = df_field['keyword']. \
        apply(lambda x: chopoff(x)). \
        apply(lambda x: x.strip()).apply(lambda x: x.strip().split('、'))
    field_keyword = {}
    field = df_field['field'].values
    keyword = df_field['merged_keywords'].values
    for fie, key in zip(field, keyword):
        field_keyword[fie] = key
    field_keyword['MESSLAH'] = ['MESSLAH', 'MESSLA']  ## some correction of data
    field_keyword['AUGILA-NAFOORA'] = ['AUGILA-NAFOORA', 'AWJILAH-NAFURAH', '102-D', '051-A']
    print(field_keyword['AUGILA-NAFOORA'])
    ## storage section
    df_dicts['storage'].columns = ['country', 'storage', 'keyword']
    df_storage = df_dicts['storage']
    storage_keyword = {}
    storage = df_storage['storage'].values
    keyword = df_storage['keyword'].values
    for stor, key in zip(storage, keyword):
        if re.search('/', key):
            storage_keyword[stor] = key.split('/')
        else:
            storage_keyword[stor] = [key.strip()]
    storage_keyword['MOLDOVA  (FALTICENI)'] = 'MOLDOVA'
    storage_keyword['CHESHIRE (HOLFORD GS)'] = 'Cheshire'
    storage_keyword['HILL TOP FARM  (CHESHIRE EXISTING)'] = 'Hill Top Farm'
    storage_keyword['HILL TOP FARM  (CHESHIRE EXPANSION)'] = 'Hill Top Farm'
    storage_keyword['KIRK RANCH  (BOBBY BURNS #1)'] = 'KIRK RANCH'
    storage_keyword['CLEMENS NE  (FRIO B)'] = 'CLEMENS,N.E.'
    ## get country according to the storage
    storage_country = {}
    storage = df_storage['storage'].values
    country = df_storage['country'].values
    for stor, coun in zip(storage, country):
        storage_country[stor] = coun

    # mark_urls = get_mark_urls()

    # ==================== reach the process section for each category==================================
    div_class_name = {'oe': {'class': 'article'},
                      'world_oil': {'id': 'news'},
                      # 'cnpc_news':{'class':'sj-main'},
                      'cnpc_news': {'id': 'contentzoom'},
                      'hart_energy': {'class': 'article-content-wrapper'},
                      'oilfield_tech': {'itemprop': "articleBody"},
                      'oil_and_gas': {'class': 'entry'},
                      'in_en_storage': {'id': 'content'},
                      'jpt_latest': {'class': 'articleBodyText'},
                      'energy_voice': {'class': 'entry-content'},
                      'gulf_oil_gas': {'class': 'newsbodytext'},
                      'energy_pedia': {'class': 'articlepreview'},
                      'up_stream': {'class': 'article-body'},
                      'oil_price': {'class': 'singleArticle__content'},
                      'inen_tech': {'id': 'content'},
                      'inen_newenergy': {'id': 'content'},
                      'natural_gas': {'class': 'article-body'},
                      'rig_zone': {'class': "divArticleText"},
                      'offshore_tech': {'class': 'c-post-single__content'},
                      'energy_year': {'class': "page-interviews"},
                      'energy_china': {'class': 'mainBody'},
                      'china_five': {'id': 'showcontent'},
                      'offshore_energy': {'class': 'wp-content'}

                      }

    for table_pair in zip(table_name, table_name_pro):
        pre_data = return_no_processed_df(table_pair[0], table_pair[1], engine)
        # print(len(pre_data))
        if len(pre_data) == 0:  ## no dataframe needed to be processed
            continue
        else:
            if re.search(r'offshore_energy', table_pair[0]):

                raw_df = pre_data.iloc[:1000]  ##make tsouhe dataframe name consistent
            else:
                raw_df = pre_data
            ## format publication time and the new content as well as source
            if table_pair[0] == 'news_oil_oe':
                raw_df['format_pub_time'] = raw_df['pub_time'] \
                    .apply(lambda x: datetime.strptime(x, "%B %d, %Y").strftime('%Y/%m/%d') if x is not None else x) \
                    .apply(lambda x: datetime.strptime(x, "%Y/%m/%d") if x is not None else x)
                raw_df['new_content'] = raw_df['content'].apply(lambda x: wash_process(x, div_class_name['oe']))
                raw_df['source'] = 'https://www.oedigital.com'
            if table_pair[0] == 'cnpc_news':
                raw_df['format_pub_time'] = raw_df['pub_time'].apply(lambda x: pormat_cnpc_news_time(x) if x else None)
                # .apply(lambda x: datetime.strptime(x, "%Y/%m/%d") if x is not None else x)
                # except ValueError:
                #    raw_df['format_pub_time'] = raw_df['pub_time'] \
                #    .apply(lambda x: datetime.strptime(x, "%m/%d").strftime('%Y/%m/%d') if x is not None else x)  \
                #    .apply(lambda x: datetime.strptime(x, "%Y/%m/%d") if x is not None else x)
                raw_df['new_content'] = raw_df['content'].apply(lambda x: wash_process(x, div_class_name['cnpc_news']))
                raw_df['source'] = 'http://news.cnpc.com.cn'
            if re.search(r'world_oil', table_pair[0]):
                raw_df['format_pub_time'] = raw_df['pub_time'] \
                    .apply(lambda x: datetime.strptime(x, "%m/%d/%Y").strftime('%Y/%m/%d') if x is not None and len(
                    x) > 4 else None) \
                    .apply(lambda x: datetime.strptime(x, "%Y/%m/%d") if x is not None else x)
                raw_df['new_content'] = raw_df['content'].apply(lambda x: wash_world_oil(x, div_class_name['world_oil']
                                                                                         ) if x is not None else x)
                raw_df['source'] = 'https://www.worldoil.com/'

            if re.search(r'hart', table_pair[0]):
                raw_df['format_pub_time'] = raw_df['pub_time'] \
                    .apply(lambda x: datetime.strptime(x, "%B %d, %Y").strftime('%Y/%m/%d') if x is not None else x) \
                    .apply(lambda x: datetime.strptime(x, "%Y/%m/%d") if x is not None else x)
                raw_df['new_content'] = raw_df['content'].apply(
                    lambda x: wash_hart_energy_process(x, div_class_name['hart_energy'])
                    )
                raw_df['source'] = 'https://www.hartenergy.com'
                # r['abstracts'] = df['title']

            if re.search(r'oilfield_tech', table_pair[0]):
                raw_df['format_pub_time'] = raw_df['pub_time'].apply(
                    lambda x: x.replace(' ', '-').replace(',', '') if x else x) \
                    .apply(lambda x: datetime.strptime(x, '%A-%d-%B-%Y-%H:%S').strftime(
                    '%Y/%m/%d %H:%S') if x is not None else x) \
                    .apply(lambda x: datetime.strptime(x, "%Y/%m/%d %H:%S") if x is not None else x)
                raw_df['new_content'] = raw_df['content'].apply(
                    lambda x: wash_process(x, div_class_name['oilfield_tech']))
                raw_df['source'] = 'https://www.oilfieldtechonology.com/'

            if re.search(r'oil_and_gas', table_pair[0]):
                raw_df['format_pub_time'] = raw_df['pub_time'] \
                    .apply(lambda x: datetime.strptime(x, "%B %d, %Y").strftime('%Y/%m/%d') if x is not None else x) \
                    .apply(lambda x: datetime.strptime(x, "%Y/%m/%d") if x is not None else x)
                raw_df['new_content'] = raw_df['content'].apply(
                    lambda x: wash_oil_gas_process(x, div_class_name['oil_and_gas']) if x is not None else x)
                raw_df['source'] = 'https://www.oilandgas360.com/'

            if re.search(r'in_en_storage', table_pair[0]):
                raw_df['format_pub_time'] = raw_df['pub_time'] \
                    .apply(lambda x: datetime.strptime(x, "%Y-%m-%d").strftime('%Y/%m/%d') if x is not None else x) \
                    .apply(lambda x: datetime.strptime(x, "%Y/%m/%d") if x is not None else x)
                raw_df['new_content'] = raw_df['content'].apply(
                    lambda x: wash_process(x, div_class_name['in_en_storage']))
                raw_df['source'] = 'https://www.in-en.com/tag/储气库'
            if re.search(r'jpt', table_pair[0]):
                # raw_df['format_pub_time'] = raw_df['pub_time'] \
                #    .apply(lambda x: datetime.strptime(x, "%d %B %Y").strftime('%Y/%m/%d') if x is not None else x) \
                #    .apply(lambda x: datetime.strptime(x, "%Y/%m/%d") if x is not None else x)
                # raw_df['new_content'] = raw_df['content'].apply(
                #    lambda x: wash_jpt_process(x, div_class_name['jpt_latest']) if x is not None else x)
                # raw_df['source'] = 'https://pubs.spe.org/'
                raw_df['format_pub_time'] = raw_df['pub_time'].apply(lambda x: datetime.strptime(x, '%B %d, %Y'))
                raw_df['new_content'] = raw_df['content'].apply(
                    lambda x: BeautifulSoup(x, 'lxml').find_all('p') if x else None)
                for i in range(len(raw_df)):
                    raw_df['new_content'].values[i] = raw_df['new_content'].values[i]. \
                        insert(0, raw_df['pre_title'].values[i])
                raw_df['pre_title'] = None
                raw_df['source'] = 'https://jpt.spe.org/latest-news'
            if re.search(r'energy_voice', table_pair[0]):
                raw_df['format_pub_time'] = raw_df['pub_time'] \
                    .apply(lambda x: datetime.strptime(x, "%d/%m/%Y").strftime('%Y/%m/%d') if x is not None else x) \
                    .apply(lambda x: datetime.strptime(x, "%Y/%m/%d") if x is not None else x)
                raw_df['new_content'] = raw_df['content'].apply(
                    lambda x: wash_energy_voice(x, div_class_name['energy_voice']) if x is not None else x)
                raw_df['source'] = 'https://www.energyvoice.com/oilandgas/'
            if re.search(r'gulf_oil_gas', table_pair[0]):
                raw_df['format_pub_time'] = raw_df['pub_time'].apply(lambda x: datetime.strptime(x, '%m/%d/%Y')) \
                    .apply(lambda x: x if datetime.now(dt.timezone.utc).date() > x \
                    else datetime.now(dt.timezone.utc).date()) \
                    .apply(lambda x: x.strftime('%Y/%m/%d') if x is not None else None) \
                    .apply(lambda x: datetime.strptime(x, "%Y/%m/%d") if x is not None else x)
                raw_df['new_content'] = raw_df['content'].apply(
                    lambda x: wash_gulf_oilgas(x, div_class_name['gulf_oil_gas']) if x is not None else x)
                raw_df['source'] = 'https://www.gulfoilandgas.com'
            if re.search(r'energy_pedia', table_pair[0]):
                raw_df['format_pub_time'] = raw_df['pub_time'].apply(lambda x: datetime.strptime(x, "%d %b %y")) \
                    .apply(lambda x: x if datetime.now(dt.timezone.utc).date() > x \
                    else datetime.now(dt.timezone.utc).date())
                ## .apply(lambda x: datetime.strptime(x, "%d %b %y").strftime
                ## ('%Y/%m/%d') if x is not None  else None) \
                ##.apply(lambda x: datetime.strptime(x, "%Y/%m/%d") if x is not None else x)
                # .apply(lambda x: datetime.strptime(x, "%d %b %y")) \
                # .apply(lambda x: x if datetime.now(dt.timezone.utc).date()> x \
                # else datetime.now(dt.timezone.utc).date())
                raw_df['new_content'] = raw_df['content'].apply(
                    lambda x: wash_energy_pedia(x, div_class_name['energy_pedia']) if x is not None else x)
                raw_df['source'] = 'https://www.energy-pedia.com/'
            if re.search(r'up_stream', table_pair[0]):
                raw_df['format_pub_time'] = raw_df['pub_time'].apply(lambda x: datetime.strptime(x, '%d %B %Y %H:%M') \
                    if x is not None else x)
                raw_df['new_content'] = raw_df['content'] \
                    .apply(lambda x: wash_upstream(x, div_class_name['up_stream']) if x is not None else x)
                raw_df.dropna(axis=0, subset=['new_content'], inplace=True)

                main_img = raw_df['preview_img_link'].apply(lambda x: upstream_main_img(x) if x is not None else x)
                # to avoid main_img is none then produce none content
                for i in range(len(main_img)):
                    if main_img.values[i] and len(main_img.values[i]) >= 1:
                        raw_df['new_content'].values[i] = main_img.values[i] + raw_df['new_content'].values[i]
                raw_df['source'] = 'http://upstreamonline.com'
            if re.search(r'oil_price', table_pair[0]):
                raw_df['format_pub_time'] = raw_df['pub_time'].apply(lambda x: datetime.strptime(x, '%b %d, %Y %H:%M') \
                    if x is not None else x)
                raw_df['new_content'] = raw_df['content'].apply(
                    lambda x: wash_oil_price(x, div_class_name['oil_price']) if x is not None else x)
                raw_df['source'] = 'https://oilprice.com/'

            if re.search(r'inen_tech', table_pair[0]) or re.search(r'inen_newenergy', table_pair[0]):
                raw_df['format_pub_time'] = raw_df['pub_time'] \
                    .apply(lambda x: datetime.strptime(x, "%Y-%m-%d").strftime('%Y/%m/%d') if x is not None else x) \
                    .apply(lambda x: datetime.strptime(x, "%Y/%m/%d") if x is not None else x)
                raw_df['new_content'] = raw_df['content'].apply(
                    lambda x: wash_inen_tech(x, div_class_name['inen_tech']))
                raw_df['source'] = 'https://oil.in-en.com/'
            if re.search(r'drill_contractor', table_pair[0]):
                raw_df['format_pub_time'] = raw_df['pub_time'] \
                    .apply(lambda x: datetime.strptime(x, "%b %d, %Y").strftime('%Y/%m/%d') if x is not None else x) \
                    .apply(lambda x: datetime.strptime(x, "%Y/%m/%d") if x is not None else x)
                raw_df['new_content'] = raw_df['content'].apply(lambda x: wash_drill_contractor(x))
                raw_df['source'] = 'https://www.drillingcontractor.org'
            if re.search(r'rog_tech', table_pair[0]):
                raw_df['format_pub_time'] = raw_df['pub_time'] \
                    .apply(lambda x: datetime.strptime(x, "%B %d, %Y").strftime('%Y/%m/%d') if x is not None else x) \
                    .apply(lambda x: datetime.strptime(x, "%Y/%m/%d") if x is not None else x)
                raw_df['new_content'] = raw_df['content'].apply(lambda x: wash_drill_contractor(x) if x else x)
                raw_df['source'] = 'https://rogtecmagazine.com'
            if re.search(r'natural_gas', table_pair[0]):
                raw_df['new_content'] = raw_df['content'] \
                    .apply(lambda x: wash_natural_gas(x, div_class_name['natural_gas']) if x else x)
                raw_df['format_pub_time'] = raw_df['pub_time'] \
                    .apply(lambda x: datetime.strptime(x, "%B %d, %Y").strftime('%Y/%m/%d') if x is not None else x) \
                    .apply(lambda x: datetime.strptime(x, "%Y/%m/%d") if x is not None else x)
                raw_df['source'] = 'https://www.naturalgasintel.com'
            if re.search(r'rig_zone', table_pair[0]):
                raw_df['preview_img_link'] = raw_df['preview_img_link'] \
                    .apply(lambda x: BeautifulSoup(x, 'lxml') if x else None).apply(lambda x: x.img if x else None)
                raw_df['format_pub_time'] = raw_df['pub_time'] \
                    .apply(lambda x: datetime.strptime(x, '%A, %B %d, %Y') if x else x)
                raw_df['new_content'] = raw_df['content'] \
                    .apply(lambda x: wash_rig_zone(x, div_class_name['rig_zone']))
                concate_img_content(raw_df)
                raw_df['preview_img_link'] = None
                raw_df['source'] = 'https://www.rigzone.com/news/'

            if table_pair[0] == 'offshore_tech':
                raw_df['new_content'] = raw_df['content']. \
                    apply(lambda x: wash_offshore_tech(x, {'class': 'c-post-single__content'}))
                raw_df['img_content'] = raw_df['categories'].apply(lambda x: change_src_img(x) if x else None)
                concate_offshore_img_content(raw_df)
                raw_df['format_pub_time'] = raw_df['pub_time'].apply(lambda x: datetime.strptime(x, '%d %b %Y'))
                raw_df['categories'] = None
                raw_df['author'] = None
                raw_df['source'] = 'https://www.offshore-technology.com/latest-news'
            if re.search(r'energy_year', table_pair[0]):
                raw_df['format_pub_time'] = raw_df['pub_time']. \
                    apply(lambda x: datetime.strptime(x.replace('-', '').strip(), '%B %d, %Y'))
                raw_df['new_content'] = raw_df['content'] \
                    .apply(lambda x: wash_energy_year(x, div_class_name['energy_year']))
                raw_df['source'] = 'https://theenergyyear.com/news/'

            if re.search(r'energy_china', table_pair[0]):
                raw_df['format_pub_time'] = raw_df['pub_time'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
                raw_df['new_content'] = raw_df['content'].apply(lambda x: wash_energy_china(x, {'class': 'mainBody'}))
                raw_df['source'] = 'http://cn.energychinaforum.com/news'

            if re.search(r'china_five', table_pair[0]):
                raw_df['new_content'] = raw_df['content']. \
                    apply(lambda x: wash_china_five(x, {'id': 'showcontent'}))
                raw_df['format_pub_time'] = raw_df['pub_time'] \
                    .apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
                raw_df['source'] = 'https://www.china5e.com/news'

            if table_pair[0] == 'offshore_energy':
                raw_df['new_content'] = raw_df['content']. \
                    apply(lambda x: wash_offshore_energy(x, {'class': 'wp-content'}))
                raw_df['format_pub_time'] = raw_df['pub_time'] \
                    .apply(lambda x: ''.join(x.split(',')[:2])) \
                    .apply(lambda x: datetime.strptime(x, '%B %d %Y'))

                raw_df['source'] = 'https://www.offshore-energy.biz/news/'

            if re.search(r'jwn_energy', table_pair[0]):
                raw_df['preview_img_link'] = 'https://www.jwnenergy.com' + raw_df['preview_img_link']
                raw_df['new_content'] = raw_df['content']. \
                    apply(lambda x: wash_jwn_energy(x, {'itemprop': 'articleBody'}) if x else x)
                raw_df['format_pub_time'] = raw_df['pub_time'] \
                    .apply(lambda x: ''.join(x.split(' ')[:4])[:-1] if x else x) \
                    .apply(lambda x: datetime.strptime(x, '%A,%B%d,%Y') if x else x)
                raw_df['source'] = 'https://www.jwnenergy.com'

            if re.search(r'iran_oil_gas', table_pair[0]):
                raw_df['new_content'] = raw_df['content']. \
                    apply(lambda x: wash_iran_oilgas(x, attrs={'id': 'newsbody'}) if x else x)
                raw_df['format_pub_time'] = raw_df['pub_time'].apply(lambda x: datetime.strptime(x, '%d %B, %Y'))
                raw_df['source'] = 'http://www.iranoilgas.com/default'
            ## new image url section
            # if re.search(r'hart',table_pair[0]):
            #     raw_df['img_urls_new'] = raw_df['new_content'].apply(lambda x: extract_hart_energy_img_links(x))
            # else:
            if re.search(r'neng_yuan', table_pair[0]):
                raw_df['format_pub_time'] = raw_df['pub_time'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
                raw_df['new_content'] = raw_df['content'].apply(lambda x: wash_nengyuan(x) if x else None)
                raw_df['source'] = 'http://www.china-nengyuan.com/'
            if re.search(r'wood_mac', table_pair[0]):
                raw_df['format_pub_time'] = raw_df['pub_time'] \
                    .apply(lambda x: datetime.strptime(x, '%d %B %Y'))
                raw_df['new_content'] = raw_df['content'].apply(lambda x: wash_woodmac(x) if x else None)
                raw_df['source'] = 'https://www.woodmac.com/'

            if re.search(r'rystad_energy', table_pair[0]):
                raw_df['format_pub_time'] = raw_df['pub_time'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
                raw_df['new_content'] = raw_df['content'].apply(lambda x: wash_rystadenergy(x))
                raw_df['source'] = 'https://www.rystadenergy.com/'

            if re.search(r'westwood_energy', table_pair[0]):
                raw_df['format_pub_time'] = raw_df['pub_time'] \
                    .apply(lambda x: datetime.strptime(x, '%B %d, %Y') if x else None)
                raw_df['new_content'] = raw_df['content'].apply(lambda x: wash_westwood(x) if x else None)
                raw_df['source'] = 'https://www.westwoodenergy.com'

            if re.search(r'iea_news', table_pair[0]):
                raw_df['format_pub_time'] = raw_df['pub_time'] \
                    .apply(lambda x: datetime.strptime(x, '%d %B %Y') if x else None)
                raw_df['new_content'] = raw_df['content'].apply(lambda x: wash_ieanews(x) if x else None)
                raw_df['source'] = 'https://www.iea.org/'

            if re.match(r'weixin', table_pair[0]):
                raw_df['new_content'] = raw_df['content'].apply(lambda x: wash_weixin(x) if x else None)
                raw_df['source'] = raw_df['author']
                raw_df['format_pub_time'] = format_weixin_pubtime(raw_df)

                change_img_links(raw_df)
                # except:
                #    pass

            if re.search(r'up_stream', table_pair[0]):

                raw_df['img_urls_new'] = raw_df['preview_img_link'].apply(
                    lambda x: upstream_main_img(x) if x is not None else x)
                raw_df['preview_img_link'] = raw_df['preview_img_link'].apply(lambda x: upstream_preview_img(x))
            elif re.search(r'weixin', table_pair[0]):
                raw_df['img_urls_new'] = raw_df['new_content'].apply(
                    lambda x: extract_weixin_img_links(x) if x is not None else x)
            else:
                raw_df['img_urls_new'] = raw_df['new_content'].apply(
                    lambda x: extract_img_links(x) if x is not None else x)
            ## crawl time formating
            raw_df['format_crawl_time'] = raw_df['crawl_time'].apply(lambda x: x.strip()[:10]) \
                .apply(lambda x: datetime.strptime(x, "%m/%d/%Y").strftime('%Y/%m/%d')) \
                .apply(lambda x: datetime.strptime(x, "%Y/%m/%d"))
            df = raw_df[['id', 'author', 'pre_title', 'categories', 'preview_img_link',
                         'title', 'url', 'new_content', 'img_urls_new',
                         'format_pub_time', 'format_crawl_time']]

            ## country keyword section
            df['country_keyword'] = df['new_content'].astype('str'). \
                apply(lambda x: match_keyword(x, country_keywords_pair))
            ## region keyword sections
            ## perform the matching according to the region keywords
            df['region_keywords'] = df['new_content'].astype('str') \
                .apply(lambda x: match_keyword(x, state_keywords_pair))

            ## determin the region according to the country keyword
            df['regions_country'] = df['country_keyword'] \
                .apply(lambda x: match_country_region(x, country_region))
            print('reach to process company section')
            ## company sections
            df['company_keyword'] = df['new_content'].astype('str') \
                .apply(lambda x: match_company(x, company_keyword_pair))
            df['business_company'] = df['company_keyword']. \
                apply(lambda x: match_country_region(x, company_business))
            df['country_matched_by_company'] = df['company_keyword']. \
                apply(lambda x: match_country_region(x, company_country))

            ## topic section
            df['topic_keyword'] = df['new_content'].astype('str').apply(lambda x: match_topic(x, topic_keywords))
            df['topic_keyword'] = df['business_company'] + df['topic_keyword']
            df['subcategory_by_topic'] = df['topic_keyword']. \
                apply(lambda x: match_country_region(x, topic_subcategory))

            ##field section
            df['field_keyword'] = df['new_content'].astype('str') \
                .apply(lambda x: match_company(x, field_keyword))

            ## storage section
            df['storage_keyword'] = df['new_content'] \
                .apply(lambda x: match_storage(x, storage_keyword))
            df['country_storage'] = df['storage_keyword'] \
                .apply(lambda x: match_country_region(x, storage_country))
            ## mark or not
            if re.search(r'^news', table_pair[0]):
                mark_urls = get_mark_urls()
                df['mark_note_by_url'] = df['url'].apply(lambda x: mark_title(x, mark_urls))
            # elif re.search(r'cnpc', table_pair[0]) :
            #   mark_titles = mark_cnpc_hot()
            #    df['mark_note_by_url'] = df['title'].apply(lambda x: mark_title(x, mark_titles))
            elif re.search(r'world_oil', table_pair[0]):
                mark_urls = get_world_oil_hot()
                df['mark_note_by_url'] = df['url'].apply(lambda x: mark_title(x, mark_urls))
            elif re.search(r'hart', table_pair[0]):
                mark_urls = get_hart_energy_hot()
                df['mark_note_by_url'] = df['url'].apply(lambda x: mark_title(x, mark_urls))
            elif re.search(r'oilfield_tech', table_pair[1]):
                df['mark_note_by_url'] = None
            elif re.search(r'oil_and_gas', table_pair[1]):
                mark_titles = get_oil_gas_hot()
                df['mark_note_by_url'] = df['title'].apply(lambda x: mark_title(x, mark_titles) if x is not None else x)
            elif re.search(r'in_en_storage', table_pair[1]) \
                    or re.search(r'jpt_latest_pro', table_pair[1]) \
                    or re.search(r'energy_voice', table_pair[1]) \
                    or re.search(r'gulf_oil_gas', table_pair[1]) \
                    or re.search(r'energy_pedia', table_pair[1]) \
                    or re.search(r'up_stream', table_pair[1]) \
                    or re.search(r'oil_price', table_pair[1]) \
                    or re.search(r'inen_tech', table_pair[1]) \
                    or re.search(r'inen_newenergy', table_pair[1]) \
                    or re.search(r'drill_contractor', table_pair[1]) \
                    or re.search(r'rog_tech', table_pair[1]) \
                    or re.search(r'natural_gas', table_pair[1]) \
                    or re.search(r'rig_zone', table_pair[1]) \
                    or re.search(r'offshore_tech', table_pair[1]) \
                    or re.search(r'energy_year', table_pair[1]) \
                    or re.search(r'energy_china', table_pair[1]) \
                    or re.search(r'china_five', table_pair[1]) \
                    or re.search(r'offshore_energy', table_pair[1]) \
                    or re.search(r'jwn_energy', table_pair[1]) \
                    or re.search(r'iran_oilgas', table_pair[1]) \
                    or re.search(r'neng_yuan', table_pair[1]) \
                    or re.search(r'wood_mac', table_pair[1]) \
                    or re.search(r'rystad_energy', table_pair[1]) \
                    or re.search(r'westwood_energy', table_pair[1]) \
                    or re.search(r'iea_news', table_pair[1]) \
                    or re.search(r'cnpc', table_pair[1]) \
                    or re.match(r'weixin', table_pair[1]):
                # or re.search(r'wood_mac',table_pair[1]):
                df['mark_note_by_url'] = None
            # print('reach to post process of data')
            ##post processgit
            df['regions'] = df['region_keywords'] + df['regions_country']
            df['country'] = df['country_keyword']
            df['company_merged'] = df['company_keyword'].apply(lambda x: add_same_key(x))
            df['regions_merged'] = df['regions'].apply(lambda x: add_same_key(x))
            df['country_merged'] = df['country'].apply(lambda x: add_same_key(x))
            df['topic_merged'] = df['topic_keyword'].apply(lambda x: add_same_key(x))
            df['subcategory_merged'] = df['subcategory_by_topic'].apply(lambda x: add_same_key(x))
            df['country_matched_by_company_merged'] = df['country_matched_by_company'].apply(lambda x: add_same_key(x))

            df['new_content'] = raw_df['new_content'] \
                .apply(
                lambda x: '\n'.join([str(ele).strip() for ele in x]) if x is not None and isinstance(x, list) else str(
                    x))

            df['topic_merged'] = df['topic_merged'].astype('str').apply(lambda x: remove_intell_topic(x))
            df['topic_merged'] = df['topic_merged'].astype('str')
            spend_time = round(time.time() - start_time, 1)
            print('spend time', spend_time, ' to process data', df.info(), table_pair[0])

            df['source'] = raw_df['source']

            ##abastracts sections
            # if re.search(r'cnpc', table_pair[0]) or re.search(r'^news', table_pair[0]):
            df['abstracts'] = df['new_content'].apply(
                lambda x: re.sub(r'<img .+>', '', str(x))[:340] if x is not None else re.sub(r'[]', '', x))
            # else:
            #     df['abstracts'] = df['pre_title']
            ##cnpc author section
            if re.search(r'cnpc_news', table_pair[0]):
                df['author'] = None
            # df['abstract'] = df['abstracts'].apply(lambda x:x[:340])
            result = df[['source', 'title', 'abstracts', 'preview_img_link', 'url', 'format_pub_time',
                         'author', 'new_content', 'categories',
                         'img_urls_new', 'format_crawl_time', 'regions_merged',
                         'country_merged', 'company_keyword', 'country_matched_by_company_merged',
                         'subcategory_merged', 'topic_merged', 'field_keyword', 'storage_keyword', 'mark_note_by_url'
                         ]]
            result['orig_id'] = df['id']
            # print(result.head(),result.columns,result.info(),result[0:1].values)
            result['preview_img_link'] = result['preview_img_link'].astype('str')
            result['img_urls_new'] = result['img_urls_new'].astype('str')
            result['regions_merged'] = result['regions_merged'].astype('str')
            result['country_merged'] = result['country_merged'].astype('str')
            result['company_keyword'] = result['company_keyword'].astype('str')
            result['country_matched_by_company_merged'] = result['country_matched_by_company_merged'].astype('str')
            result['subcategory_merged'] = result['subcategory_merged'].astype('str')
            result['field_keyword'] = result['field_keyword'].astype('str')
            result['storage_keyword'] = result['storage_keyword'].astype('str')
            result['mark_note_by_url'] = result['mark_note_by_url'].astype('str')
            # test = result[0:1].values
            result.drop_duplicates(subset=['title'], inplace=True)
            # print(table_name_pro)
            ##update column of field_keyword
            # result.to_sql('temp_table', engine, if_exists='replace')
            # sql = f"""UPDATE {table_pair[1]}  t1
            #             INNER JOIN temp_table t2  ON t1.orig_id = t2.orig_id
            #             SET t1.field_keyword = t2.field_keyword"""
            # with engine.begin() as conn:
            #     conn.execute(sql)
            # try:
            # print(result['source'].values[0])
            # print(result['format_pub_time'].values[0],type(result['format_pub_time'].values[0]))
            result.to_sql(table_pair[1], engine, if_exists='append', index=False, chunksize=1)
            # print('processed ', table_pair[1])
            # except:
            #    print('table',' is not processed',table_pair[0])
            #    continue
