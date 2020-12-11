import re
import ast
from bs4 import BeautifulSoup
import pandas as pd
import requests as req
from bs4 import Tag


def wash_process(x):
    '''
    '''
    contents = []
    soup = BeautifulSoup(x, 'lxml')
    ancestor = soup.find('div', attrs={'class': 'article'})
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
    import requests as req
    from bs4 import BeautifulSoup
    url = 'https://www.oedigital.com'
    headers= {'user-agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0'}
    res = req.get(url,headers=headers)
    soup = BeautifulSoup(res.text,'lxml')
    hrefs = soup.find_all('a',attrs={'class':'snippet-flex'})
    for href in hrefs:
        mark_urls.append(href.attrs['href'])
    return mark_urls


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