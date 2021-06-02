import re
import ast
from bs4 import BeautifulSoup
from bs4.element import NavigableString
import pandas as pd
import requests as req
from bs4 import Tag

from datetime import datetime
from datetime import timedelta


#
# def wash_oe_cnpc_hart_process(x,class_name):
#     '''
#     '''
#     contents = []
#     soup = BeautifulSoup(x, 'lxml')
#     ancestor = soup.find('div', attrs={'class': class_name})
#     for desc in ancestor.descendants:
#         if desc.name == 'img' and desc.has_attr('src'):
#             contents.append(desc)
#         elif desc.name == 'p' and not desc.has_attr('class'):
#             contents.append(desc.text.replace(u'\xa0', u''))
#
#     return contents

def change_img_links(df):
    #     img_links = []
    base_url = 'https://newsimg.hydross.cn/'

    for row in df.itertuples():
        for ele in row.new_content:
            img_link_local = ast.literal_eval(row.images) if row.images else None
            if isinstance(ele, Tag) and ele.name == 'img' and img_link_local \
                    and ele.has_attr('data-src'): \
                    # and ele.has_attr('alt'):
                #             print(ele)
                img_link = ele.attrs['data-src']
                for img_local in img_link_local:
                    if img_link == img_local['url']:
                        ele.attrs['src'] = base_url + img_local.get('path')
                        #                                 img_links.append((str(img_link), ele.attrs['alt']))
                        break
        continue


def wash_world_oil(x, attrs):
    '''grab
    '''
    contents = []
    chop_index = None
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('div', attrs=attrs)

    if ancestor is not None:
        # print(list(ancestor.children))
        for child in [child for child in ancestor.children if not isinstance(child, NavigableString)]:
            #     print(child)
            if child.name == 'p' and not child.has_attr('class'):
                contents.append(child.text.replace(u'\xa0', u''))
            #     elif child.name=='p'and child.find('strong') and not child.find('strong'):
            #         break pu
            elif child.name == 'div':
                for desc in child.descendants:
                    if not isinstance(desc, NavigableString):
                        if desc.name == 'img' and desc.has_attr('src') and re.search('/media', desc.attrs['src']):
                            desc.attrs['src'] = 'https://www.worldoil.com' + desc.attrs['src']
                            contents.append(desc)
                            # contents.append(desc)
            elif child.name == 'h2' and re.search(r'Related News', str(child.string)):
                break
    try:
        if contents.index('REFERENCES'):
            chop_index = contents.index('REFERENCES')
    except:
        pass
    contents = contents[:chop_index]

    return contents


def pormat_cnpc_news_time(x):
    if re.search('-', x):
        return datetime.strptime(x, "%Y-%m-%d")
    elif len(re.findall('/', x)) == 1:
        return datetime.strptime(x, "%m/%d")
    elif len(re.findall('/', x)) == 2:
        return datetime.strptime(x, "%Y/%m/%d")


def wash_hart_energy_process(x, attrs):
    '''
    '''
    contents = []
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('div', attrs=attrs)
    if ancestor is not None:
        for child in [child for child in ancestor.children if not isinstance(child, NavigableString)][:2]:
            for desc in child.descendants:
                if desc.name == 'img' and desc.has_attr('src'):
                    desc.attrs['src'] = 'https://www.hartenergy.com' + desc.attrs['src']
                    contents.append(desc)
                if desc.name == 'p' and not desc.has_attr('class'):
                    contents.append(desc.text.replace(u'\xa0', u''))
                if desc.name == 'div' and desc.has_attr('class') and desc.attrs['class'] == "userAlready":
                    break

    return contents


def wash_process(x, attrs):
    '''
    '''
    contents = []
    if x is not None and isinstance(x, str):
        soup = BeautifulSoup(x, 'lxml')
        ancestor = soup.find('div', attrs=attrs)
        if ancestor is not None:
            for desc in ancestor.descendants:
                if desc.name == 'img' and desc.has_attr('src'):
                    contents.append(desc)
                elif desc.name == 'p' and not desc.has_attr('class'):
                    contents.append(desc.text.replace(u'\xa0', u''))

    return contents


def wash_oil_gas_process(x, attrs):
    '''
    '''
    contents = []
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('div', attrs=attrs)

    for desc in ancestor.descendants:
        if desc.name == 'img' and desc.has_attr('src') and re.search(r'/site', desc.attrs['src']):
            desc.attrs['src'] = 'https://www.oilandgas360.com' + desc.attrs['src']
            contents.append(desc)
        elif desc.name == 'p' and not desc.has_attr('class') \
                and not 'a' in [child.name for child in desc.children]:
            contents.append(desc.text.replace(u'\xa0', u''))

    return contents


def wash_jpt_process(x, attrs):
    '''
    '''
    contents = []
    if x is not None:
        soup = BeautifulSoup(x, 'lxml')
        ancestor = soup.find('div', attrs=attrs)
        # if ancestor is not None:
        for desc in ancestor.descendants:
            if desc.name == 'img' and desc.has_attr('src') and re.search(r'/media', desc.attrs['src']):
                desc.attrs['src'] = 'https://pubs.spe.org' + desc.attrs['src']
                contents.append(desc)
            elif desc.name == 'p' and not desc.has_attr('class'):
                contents.append(desc.text.replace(u'\xa0', u''))

    return contents


def wash_energy_voice(x, attrs):
    contents = []
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('div', attrs=attrs)
    for desc in ancestor.descendants:
        if desc.name == 'img' and desc.has_attr('src') and desc.has_attr('srcset'):
            contents.append(desc)
        elif desc.name == 'p' \
                and not desc.has_attr('class') \
                and not re.search(r'12.50 per month', desc.text) \
                and not desc.parent.attrs.get('id') == 'recommended-popout':
            contents.append(desc.text.replace(u'\xa0', u''))
    return contents


def wash_gulf_oilgas(x, attrs):
    contents = []
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('div', attrs=attrs)
    for desc in ancestor.descendants:
        if desc.name == 'img' and desc.has_attr('src'):
            #         desc.attrs['src'] = desc.attrs['src']
            contents.append(desc)
        if isinstance(desc, NavigableString):
            contents.append(re.sub(r'\r\n', '', str(desc)))
    return contents


def wash_energy_pedia(x, attrs):
    contents = []
    #     index_ori= None
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('div', attrs=attrs)
    for desc in ancestor.descendants:
        #     if desc.name == 'img' and desc.has_attr('src') :
        #         contents.append(desc)
        if desc.name == 'p':
            contents.append(desc.text)

    try:
        index_ori = contents.index('Original article link')
        return contents[:index_ori]
    except:
        return contents


def wash_upstream(x, attrs):
    contents = []
    soup = BeautifulSoup(x, 'lxml')
    if soup:
        #     break
        ancestor = soup.find('div', attrs=attrs)
        #         img_src = None
        for desc in ancestor.descendants:
            if desc.name == 'p':
                contents.append(desc.text.replace(u'\xa0', u''))
    if len(contents) > 1:
        return contents
    # else:
    #   return None


def wash_oil_price(x, attrs):
    contents = []
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('div', attrs=attrs)
    for desc in ancestor.descendants:
        if desc.name == 'img' and desc.has_attr('src') \
                and desc.attrs['src'] \
                not in [e.attrs['src'] for e in contents if isinstance(e, Tag)]:
            #         contents.append(desc)
            contents.append(desc)
        elif desc.name == 'p' and not desc.has_attr('class') \
                and not 'a' in [child.name for child in desc.children] \
                and not 'strong' in [child.name for child in desc.children]:
            contents.append(desc.text.replace(u'\xa0', u''))
    return contents


def wash_inen_tech(x, attrs):
    contents = []
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('div', attrs=attrs)
    for desc in ancestor.descendants:
        if desc.name == 'img' and desc.has_attr('src'):
            #         contents.append(desc)
            contents.append(desc)
        elif desc.name == 'p' and len(desc.text) > 0:
            contents.append(desc.text.replace(u'\xa0', u''))
    return contents


def wash_drill_contractor(x):
    contents = []
    res = ast.literal_eval(x)
    for e in res:
        soup = BeautifulSoup(e, 'lxml')
        contents.append(soup.text.replace(u'\xa0', u''))

    return contents


def wash_natural_gas(x, attrs):
    contents = []
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('div', attrs=attrs)
    for desc in ancestor.descendants:
        if desc.name == 'img' and desc.has_attr('src'):
            #         contents.append(desc)
            contents.append(desc)
        elif desc.name == 'p' and len(desc.text) > 0:
            contents.append(desc.text.replace(u'\xa0', u''))
    return contents


def wash_rig_zone(x, attrs):
    contents = []
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('div', attrs=attrs)
    for desc in ancestor.descendants:
        if desc.name == 'img' and desc.has_attr('src'):
            #         contents.append(desc)
            contents.append(desc)
        elif desc.name == 'p' and desc.text == "Other oil-market news:":
            break
        elif desc.name == 'p' and re.search(r'To contact the author', desc.text):
            break
        elif desc.name == 'p' and len(desc.text) > 1:
            contents.append(desc.text.replace(u'\xa0', u''))

    return contents


def concate_img_content(raw_df):
    '''concatet the img link with the news content
    '''

    for i in range(len(raw_df['new_content'])):
        raw_df['new_content'].values[i].insert(0, raw_df['preview_img_link'].values[i])


def concate_offshore_img_content(raw_df):
    '''concatet the img link with the news content
    '''

    for i in range(len(raw_df['new_content'])):
        raw_df['new_content'].values[i].insert(0, raw_df['img_content'].values[i])


def change_src_img(x):
    '''change src of main image
    '''
    soup = BeautifulSoup(x, 'lxml')
    img_soup = soup.img
    try:
        img_soup.attrs['src'] = img_soup.attrs['data-src']
    except:
        pass

    return img_soup


def wash_offshore_tech(x, attrs):
    contents = []
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('div', attrs=attrs)
    if ancestor:
        for desc in ancestor.descendants:
            if desc.name == 'img' and desc.has_attr('src'):
                #        break
                #        contents.append(desc)
                continue
            #         contents.append(desc)
            elif desc.name == 'p' and len(desc.text) > 1:
                contents.append(desc.text.replace(u'\xa0', u'').strip())
            elif desc.name == 'aside':
                break
    return contents


def wash_energy_year(x, attrs):
    '''wash energy year
    '''
    contents = []
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('div', attrs=attrs)
    for desc in ancestor.descendants:
        #     if desc.name == 'img' and desc.has_attr('src'):
        # #         contents.append(desc)
        #         contents.append(desc)
        if desc.name == 'p' and re.search(r'First published', desc.text):
            break
        elif desc.name == 'p' and len(desc.text) > 1:
            contents.append(desc.text.replace(u'\xa0', u''))
    return contents


def wash_energy_china(x, attrs):
    contents = []
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('div', attrs=attrs)
    for desc in ancestor.descendants:
        if desc.name == 'img' and desc.has_attr('src') and \
                not re.search('erweima', desc.attrs['src']):
            #         contents.append(desc)
            contents.append(desc)
        if desc.name == 'p' and re.search('新闻时间', desc.text):
            break
        if desc.name == 'p' and re.search('来源：', desc.text):
            continue
        elif desc.name == 'p' and len(desc.text) > 0:
            contents.append(desc.text.replace(u'\xa0', u''))
    return contents


def wash_china_five(x, attrs):
    contents = []
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('div', attrs=attrs)
    for desc in ancestor.descendants:
        if desc.name == 'img' and desc.has_attr('src'):
            contents.append(desc)
        elif desc.name == 'p' and re.search(r'责任编辑', desc.text):
            break
        elif desc.name == 'p' and len(desc.text) > 0:
            contents.append(desc.text.replace(u'\xa0', u''))
    return contents


def wash_offshore_energy(x, attributes):
    '''wash energy year
    '''
    contents = []
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('div', attrs=attributes)
    if ancestor:
        for desc in ancestor.descendants:
            if desc.name == 'img' and desc.has_attr('src'):
                contents.append(desc)
            #         contents.append(desc)
            #         if desc.name=='p' and re.search(r'First published',desc.text):
            #             break
            elif desc.name == 'p' and len(desc.text) > 1:
                contents.append(desc.text.replace(u'\xa0', u''))
            elif desc.name == 'div' and desc.has_attr('class') and \
                    'block-related-article' in desc.attrs['class']:
                break
    return contents


def wash_jwn_energy(x, attrs):
    contents = []
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('div', attrs=attrs)
    if ancestor:
        for desc in ancestor.descendants:
            if desc.name == 'img' and desc.has_attr('src'):
                desc.attrs['src'] = 'https://www.jwnenergy.com' + desc.attrs['src']
                contents.append(desc)
            elif desc.name == 'p' and len(desc.text) > 0:
                contents.append(desc.text.replace(u'\xa0', u''))
            elif desc.name == 'p' and re.search('©', desc.text):
                break
    return contents


def wash_iran_oilgas(x, attrs):
    contents = []
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('div', attrs=attrs)
    #     if ancestor:
    for desc in ancestor.descendants:
        if desc.name == 'img' and desc.has_attr('src'):
            contents.append(desc)
        elif desc.name == 'p' and len(desc.text) > 0:
            contents.append(desc.text.replace(u'\xa0', u''))
    return contents


def wash_nengyuan(x):
    contents = []
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('td')
    if ancestor:
        for desc in ancestor.descendants:
            if desc.name == 'img' and desc.has_attr('src'):
                contents.append(desc)
            elif desc.name == 'p' and len(desc.text) > 0:
                contents.append(desc.text.replace(u'\xa0', u''))
    return contents


def wash_woodmac(x):
    contents = []
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('div', attrs={'class': 'editor'})
    #     if ancestor:
    for desc in ancestor.descendants:
        if desc.name == 'img' and desc.has_attr('src'):
            contents.append(desc)
        elif desc.name == 'p' and len(desc.text) > 0:
            contents.append(desc.text.replace(u'\xa0', u''))
    return contents


def wash_rystadenergy(x):
    contents = []
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('div', attrs={'class': 'text-break'})
    #     if ancestor:
    for desc in ancestor.descendants:
        if desc.name == 'img' and desc.has_attr('src'):
            desc.attrs['src'] = 'https://www.rystadenergy.com' + desc.attrs['src']
            contents.append(desc)
        elif desc.name == 'p' and len(desc.text) > 0:
            contents.append(desc.text.replace(u'\xa0', u''))
    return contents


def wash_westwood(x):
    contents = []
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('div', attrs={'class': 'wpb_wrapper'})
    if ancestor:
        for desc in ancestor.descendants:
            if desc.name == 'img' and desc.has_attr('src'):
                contents.append(desc)
            elif desc.name == 'p' and len(desc.text) > 0:
                contents.append(desc.text.replace(u'\xa0', u''))
            elif desc.name == 'tbody':
                contents.append(desc)
    return contents


def wash_ieanews(x):
    contents = []
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('div', attrs={'class': 'm-block__content'})
    if ancestor:
        for desc in ancestor.descendants:
            if desc.name == 'img' and desc.has_attr('src'):
                contents.append(desc)
            elif desc.name == 'p' and len(desc.text) > 0:
                contents.append(desc.text.replace(u'\xa0', u''))
            elif desc.name == 'tbody':
                contents.append(desc)
    return contents


# def wash_weixin(x):
#    contents = []
#    soup = BeautifulSoup(x, 'lxml')
#    ancestor = soup.find('div',attrs={'id':'js_content'})
#    for desc in ancestor.descendants:
#        if desc.name == 'img' and desc.has_attr('src') and desc not in contents:
#            contents.append(desc)
#        if desc.name =='span' and desc.find_parent('section') and desc.text not in contents:
#            contents.append(desc.text)
#        if desc.name == 'span' and desc.find_parent('p') and desc.text not in contents:
#            contents.append(desc.text)
#        if desc.name == 'strong' and re.search(r'END',desc.text):
#            break
#    return contents

def wash_weixin(x):
    contents = []
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('div', attrs={'id': 'js_content'})
    for desc in ancestor.descendants:
        if desc.name == 'img' and (desc.has_attr('src') or desc.has_attr('data-src')) \
                and desc not in contents:
            contents.append(desc)
        if desc.name == 'span' and desc.find_parent('section') and desc.text not in contents:
            contents.append(desc.text)
        if desc.name == 'span' and desc.find_parent('p') and desc.text not in contents:
            contents.append(desc.text)
        if desc.name == 'strong' and re.search(r'END', desc.text):
            break
    return contents


def extract_weixin_img_links(x):
    img_links = []

    for ele in x:
        if isinstance(ele, Tag):
            if ele.name == 'img' \
                    and (ele.has_attr('data-src') or ele.has_attr('src')) \
                    and ele.has_attr('alt'):
                #             print(ele)
                img_link = ele.attrs['data-src'] if ele.has_attr('data-src') else None
                #                 print(img_link)
                if img_link and re.match(r'^http', img_link):
                    img_links.append((str(img_link), ele.attrs['alt']))

    return img_links


# def format_weixin_pubtime(raw):
#    format_pub_time_list = []
#    for row in raw.itertuples():

#        if re.search('/',row.pub_time):
#         print(row.pub_time)
#            month = datetime.strptime(row.pub_time,'%m/%d').month
#            day = datetime.strptime(row.pub_time,'%m/%d').day
#            format_pub_time = datetime(2021,month,day)
#         print(format_pub_time)
#            format_pub_time_list.append(format_pub_time)
#        elif  re.search('-',row.pub_time):
#            format_pub_time = datetime.strptime(row.pub_time,'%Y-%m-%d')
#            format_pub_time_list.append(format_pub_time)
#
#        else:
#            crawldate = datetime.strptime(row.crawl_time,'%m/%d/%Y %H:%M')
#            day_pub = re.search(r'\d',row.pub_time).group(0)
#            format_pub_time = crawldate-timedelta(days=int(day_pub))
#         print(format_pub_time)
#            format_pub_time_list.append(format_pub_time)
#    return format_pub_time_list
def format_weixin_pubtime(raw):
    format_pub_time_list = []
    for row in raw.itertuples():
        crawldate = datetime.strptime(row.crawl_time, '%m/%d/%Y %H:%M')
        if row.pub_time:

            if re.search('/', row.pub_time):
                #         print(row.pub_time)
                month = datetime.strptime(row.pub_time, '%m/%d').month
                day = datetime.strptime(row.pub_time, '%m/%d').day
                format_pub_time = datetime(2021, month, day)
                #         print(format_pub_time)
                format_pub_time_list.append(format_pub_time)
            elif re.search('-', row.pub_time):
                format_pub_time = datetime.strptime(row.pub_time, '%Y-%m-%d')
                format_pub_time_list.append(format_pub_time)

            elif re.search('days', row.pub_time):
                crawldate = datetime.strptime(row.crawl_time, '%m/%d/%Y %H:%M')
                day_pub = re.search(r'\d', row.pub_time).group(0)
                format_pub_time = crawldate - timedelta(days=int(day_pub))
                format_pub_time_list.append(format_pub_time)
            elif re.search('week', row.pub_time):
                crawldate = datetime.strptime(row.crawl_time, '%m/%d/%Y %H:%M')
                format_pub_time = crawldate - timedelta(days=7)
                #         print(format_pub_time)
                format_pub_time_list.append(format_pub_time)
            elif re.search('today|Today', row.pub_time):
                format_pub_time = crawldate - timedelta(days=0)

                format_pub_time_list.append(format_pub_time)
            elif re.search('Yesterday|yesterday', row.pub_time):
                format_pub_time = crawldate - timedelta(days=1)
                format_pub_time_list.append(format_pub_time)
            else:
                format_pub_time_list.append(row.pub_time)
        else:
            format_pub_time_list.append(None)
    return format_pub_time_list


def upstream_preview_img(x):
    soup = BeautifulSoup(x, 'lxml')
    if soup is not None:
        preview_img = soup.find('img').attrs.get('srcset') \
            .split(',')[1].strip().split(' ')[0] \
            if soup.find('img').attrs.get('srcset') else None

    return preview_img


def upstream_main_img(x):
    main_img = []
    soup = BeautifulSoup(x, 'lxml')
    if soup is not None:
        return main_img.append(soup.find('img').attrs.get('srcset') \
                               .split(',')[3].strip().split(' ')[0] \
                                   if soup.find('img').attrs.get('srcset') else None)


def extract_img_links(x):
    '''extract img_links from content
    '''
    img_links = []

    for ele in x:
        if isinstance(ele, Tag):
            if ele.name == 'img' and ele.has_attr('src') and ele.has_attr('alt'):
                img_link = ele.attrs['src']
                if re.match(r'^http', img_link):
                    img_links.append((str(img_link), ele.attrs['alt']))

    return img_links


# def extract_hart_energy_img_links(x):
#     '''extract img_links from content
#     '''
#     img_links = []
#     for ele in x:
#         if isinstance(ele, Tag):
#             if ele.name == 'img' and ele.has_attr('src') :
#                 img_link = ele.attrs['src']
#                 if re.match(r'^/', img_link):
#                     img_links.append(('https://www.hartenergy.com'+str(img_link), ele.attrs['alt'] if ele.attrs['alt'] else None))
#
#     return img_links

def read_xlsx(file):
    '''read all the categories sheet for keywords searching
    '''

    excel_file = pd.ExcelFile(file)
    df_dicts = {}
    for sheet in excel_file.sheet_names:
        df_dicts[sheet] = pd.read_excel(excel_file, sheet_name=sheet)

    return df_dicts


def gen_keywords_pair(df, tag_index, keywords_cols):
    '''return a tat,keywords list ot dictionaries
    args:
        tag name to label the data
    keywords_cols: keyword columns index
    '''

    keywords_pair = []
    #     print(keywords)
    #     kewords_pair
    for row in df.itertuples():
        #         print(dir(row))
        extract_keyword_pair = {}
        extract_keywords = []
        for keyword_col in keywords_cols:

            #             print(row.keyword)
            if (len(row[keyword_col].split(',')) > 1):
                #                 row[keyword_col] = row[keyword_col].strip()
                extract_keywords.extend(row[keyword_col].split(','))
            else:
                extract_keywords.append(row[keyword_col])

        extract_keyword_pair[row[tag_index]] = extract_keywords
        keywords_pair.append(extract_keyword_pair)
    return keywords_pair


def match_keyword(x, keywords_pair):
    '''
    '''
    matches = []

    for keyword_pair in keywords_pair:
        for k, values in keyword_pair.items():
            #             print(k,values)
            for val in values:
                #                 print(val,type(val))
                if re.findall(val, x):
                    #                     print(type(x))
                    matches.append((k, len(re.findall(val, x))))
    return matches


def match_country_region(xs, country_region):
    '''match country to region'''
    matches_country_region = []
    if xs is not None:
        for _ in xs:
            country, time = _
            region = country_region.get(country)
            matches_country_region.append((region, time))
    return matches_country_region


def chopoff(x):
    if x[-1] == '、' or x[-1] == '，' or x[-1] == ',':
        return x[:-1]
    else:
        return x


def match_company(x, company_keyword):
    '''match content to company keywords
    '''
    matches = []
    if x is not None:
        for company, keyword in company_keyword.items():
            for key in keyword:
                if re.findall(key, x):
                    matches.append((company, len(re.findall(key, x))))

    return matches


def rematch_keywords(xs, ys):
    '''regerate keyword
    '''
    smart_geo = []
    for x in xs:
        for y in ys:
            smart_geo.append((x, y))

    return smart_geo


def match_topic(x, topic_keyword):
    '''match content to company keywords
    '''
    matches = []
    if x is not None:
        for topic, keyword in topic_keyword.items():
            for key in keyword:
                #                 print(key)
                if not isinstance(key, tuple):
                    if re.findall(key, x) and not key == '':
                        matches.append((topic, len(re.findall(key, x))))
                else:
                    if re.findall(key[0], x) and re.findall(key[1], x):
                        matches.append(
                            (topic, min(len(re.findall(key[0], x)), len(re.findall(key[1], x))))
                        )
    #                 break
    return matches


def match_storage(x, storage_keyword):
    '''match content to company keywords
    '''
    matches = []
    if x is not None:
        for company, keyword in storage_keyword.items():
            for key in keyword:
                #                 print(type(key),key,'the field is',company)
                try:
                    #                     print(f' {key} ')
                    if len(re.findall(f' {key} ', x)) >= 1 and (not key == '') and (not key == " "):
                        matches.append((company, len(re.findall(key, x))))
                except:
                    continue
    #             break

    return matches


def get_mark_urls():
    mark_urls = []
    url = 'https://www.oedigital.com'
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0'}
    res = req.get(url, headers=headers)
    soup = BeautifulSoup(res.text, 'lxml')
    hrefs = soup.find_all('a', attrs={'class': 'snippet-flex'})
    for href in hrefs:
        mark_urls.append(href.string)
    return mark_urls


def get_world_oil_hot():
    '''get front page url'''
    ele_urls = []
    host = 'https://www.worldoil.com'
    res = req.get(host)
    soup = BeautifulSoup(res.text, 'lxml')
    cols = soup.find_all('div', attrs={'class': 'col-sm-6'})[:2]
    for col in cols:
        urls = col.find_all('a')
        for url in urls:
            ele_urls.append(url.string)
    return ele_urls


def get_hart_energy_hot():
    ele_urls = []
    host = 'https://www.hartenergy.com'
    res = req.get(host)
    soup = BeautifulSoup(res.text, 'lxml')
    latest = soup.find('div', attrs={'id': 'homepage-latest'})
    rows = latest.find_all('a')
    for row in rows[:-1]:
        ele_urls.append(row.string)
    return ele_urls


def get_oil_gas_hot():
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0'}
    titles = []
    host = 'https://www.oilandgas360.com/'
    res = req.get(host, headers=headers)
    soup = BeautifulSoup(res.text, 'lxml')
    ancestor = soup.find('div', attrs={'class': 'main-area'}).descendants
    for child in ancestor:
        if child.name == 'a' and child.has_attr('title'):
            titles.append(child.attrs['title'])
    titles = set(titles)
    return titles


def mark_cnpc_hot():
    '''compare with the tiltes'''
    titles = []
    host = 'http://news.cnpc.com.cn/toutiao/'
    res = req.get(host)
    soup = BeautifulSoup(res.text, 'lxml')
    lists = soup.find('div', attrs={'class': 'list18'})
    for row in lists.find_all('li', attrs={'class': 'ejli'}):
        title = row.find('a').text.strip()
        titles.append(title)

    return titles


def mark_title(x, mark_titles):
    mark_titles = [mark.strip().lower() for mark in mark_titles if mark]
    if x.strip().lower() in mark_titles:
        return True


def add_same_key(x):
    dict_ele = {}

    if x is not None and len(x) >= 1:
        for e in x:
            #             print(e)
            if dict_ele.get(e[0]) is not None:
                dict_ele[e[0]] = e[1] + dict_ele.get(e[0])
            else:
                dict_ele[e[0]] = e[1]
        return list(dict_ele.items())
    else:
        return x


def remove_intell_topic(x):
    '''remove the intelligent tag labeled
    '''
    new_x = []
    x_str_s = ast.literal_eval(x)
    for x_str in x_str_s:
        #         print(x_str,type(x_str))
        if not re.search('智能化', str(x_str)):
            new_x.append(x_str)

    return new_x