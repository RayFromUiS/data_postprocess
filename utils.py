import re
import ast
from bs4 import BeautifulSoup
from bs4.element import NavigableString
import pandas as pd
import requests as req
from bs4 import Tag

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
                            contents.append(desc)
            elif child.name == 'h2' and re.search(r'Related News', str(child.string)):
                break
    try:
        if contents.index('REFERENCES'):
            chop_index = contents.index('REFERENCES')
    except :
        pass
    contents = contents[:chop_index]

    return contents

def wash_hart_energy_process(x,attrs):
    '''
    '''
    contents = []
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('div',attrs=attrs)
    for child in [child for child in ancestor.children if not isinstance(child,NavigableString)][:2]:
        for desc in child.descendants:
            if desc.name == 'img' and desc.has_attr('src'):
                contents.append(desc)
            if desc.name == 'p' and not desc.has_attr('class'):
                contents.append(desc.text.replace(u'\xa0', u''))
            if desc.name == 'div' and desc.has_attr('class') and desc.attrs['class']=="userAlready":
                break

    return contents

def wash_process(x,attrs):
    '''
    '''
    contents = []
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('div', attrs=attrs)
    for desc in ancestor.descendants:
        if desc.name == 'img' and desc.has_attr('src'):
            contents.append(desc)
        elif desc.name == 'p' and not desc.has_attr('class'):
            contents.append(desc.text.replace(u'\xa0', u''))

    return contents



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

def match_keyword(x,keywords_pair):
    '''
    '''
    matches = []

    for keyword_pair in keywords_pair:
        for k,values in keyword_pair.items():
#             print(k,values)
            for val in values:
#                 print(val,type(val))
                if re.findall(val,x):
#                     print(type(x))
                    matches.append((k,len(re.findall(val,x))))
    return matches

def match_country_region(xs,country_region):
    '''match country to region'''
    matches_country_region =[]
    if xs is not None:
        for _ in xs:
            country,time = _
            region = country_region.get(country)
            matches_country_region.append((region,time))
    return matches_country_region


def chopoff(x):
    if x[-1] =='、' or x[-1]=='，' or x[-1]==',':
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

def match_topic(x,topic_keyword):
    '''match content to company keywords
    '''
    matches =[]
    if x is not None:
        for topic,keyword in topic_keyword.items():
            for key in keyword:
#                 print(key)
                if not isinstance(key,tuple):
                    if re.findall(key,x) and not key=='':
                        matches.append((topic,len(re.findall(key,x))))
                else:
                    if re.findall(key[0],x) and re.findall(key[1],x):
                        matches.append(
                            (topic,min(len(re.findall(key[0],x)),len(re.findall(key[1],x))))
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
    mark_urls =[]
    url = 'https://www.oedigital.com'
    headers= {'user-agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0'}
    res = req.get(url,headers=headers)
    soup = BeautifulSoup(res.text,'lxml')
    hrefs = soup.find_all('a',attrs={'class':'snippet-flex'})
    for href in hrefs:
        mark_urls.append(href.attrs['href'])
    return mark_urls

def get_world_oil_hot():
    '''get front page url'''
    ele_urls = []
    host='https://www.worldoil.com'
    res = req.get(host)
    soup = BeautifulSoup(res.text,'lxml')
    cols = soup.find_all('div',attrs={'class':'col-sm-6'})[:2]
    for col in cols:
        urls = col.find_all('a')
        for url in urls:
            ele_urls.append(host+url['href'])
    return ele_urls


def get_hart_energy_hot():
    ele_urls = []
    host = 'https://www.hartenergy.com'
    res = req.get(host)
    soup = BeautifulSoup(res.text, 'lxml')
    latest = soup.find('div', attrs={'id': 'homepage-latest'})
    rows = latest.find_all('a')
    for row in rows[:-1]:
        ele_urls.append(host + row['href'])
    return ele_urls


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

def mark_url(x, mark_urls):
    for mark_url in mark_urls:
        return str(mark_url).strip() == str(x).strip()


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
        if not re.search('智能化',str(x_str)):
            new_x.append(x_str)

    return new_x