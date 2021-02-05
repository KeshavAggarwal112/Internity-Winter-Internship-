#!/usr/bin/env python
# coding: utf-8

# In[38]:


import pandas as pd
import numpy as np
from bs4 import BeautifulSoup as bs
import requests
import urllib.request
import json
from tqdm.notebook import tqdm
import pycountry 
import re
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objs as go
from plotly.offline import  iplot


# In[39]:


def html_to_df(urls, clean_empty = False , attrs = {}, helper = None):
    '''
    Input:
        urls : url's list from which the tabel need to be scrapping.
        clean_empty : if remove completly empty column.
        attrs : html attr. dict tag while more then on table e.g. {'class' : 'class_name'}.
        helper : helper function for cleaning df.
    Output:
        df : dataframe
    '''
    df_l = []
    for url in urls:
        html_content = requests.get(url).text
        soup = bs(html_content, "lxml")
        table = str(soup.find("table", attrs=attrs))
        df = pd.read_html(str(table))[0]
    
        if clean_empty :
            df = df.loc[:, ~df.isnull().all(axis = 0)]
        
        if helper:
            df = helper(df) 
        df_l.append(df)
    return pd.concat(df_l,ignore_index=True)

def loc_dict_maker(unq_series):
    '''
    input:
        series: series of unique ip address.
    output:
        res: dict contain country code, lat, long.
    '''
    def ip_loc(x):
        '''
        input:
            x : ip address
        output:
            dictionary which contain country,latitude and longitude.
        '''
        res = {}
        url = "https://geolocation-db.com/jsonp/"+x
        with urllib.request.urlopen(url) as url:
            data = json.loads(url.read().decode().split("(")[1].strip(")"))
        res = {"country_code":data["country_code"], 'latitude':data['latitude'],'longitude':data['longitude']}
        try:
            res['alpha_3'] = pycountry.countries.get(alpha_2=country).alpha_3
        except:
            res['alpha_3'] = 'Not found'
        return res
    
    result = {}
    for ip in tqdm(unq_series):
        result[ip] = ip_loc(ip)
    return result


def log_parser_re(str_):
    '''
    input:
        str_ : log string.
    output:
        return a dictionary which contain all element of log string.
    '''
    finder = [r'(?P<ip>\A\w+[.]\w+[.]+\w+[.]+\w+)',
              r'(?P<RFC931>\S+)',
              r'(?P<user>\S+)',
              r'\[(?P<date>\d{2}/[a-zA-Z]{3}/\d{4}:\d{2}:\d{2}:\d{2}) (?P<gmt>[+-]\d{4})]',
              r'"(?P<action>.*) HTTP/\d*\.*\d*"',
              r'(?P<status>[0-9]*)',
              r'(?P<size>\S*)',
              r'"(?P<referrer>.*)" "(?P<browser>.*)"']
    m = re.search(' '.join(finder),str_)
    return m.groupdict()



def log_df(df,col_name ,columns = [] ):
    '''
    input: 
        df : dataframe
        col_name : column name of dataframe on which we need to apply function.
        columns : column name for new dataframe.
    output:
        return new dataframe.
    '''
    def log_parser(str_):
        '''
        input:
            str_ : log string.
        output:
            return a dictionary which contain all element of log string.
        '''
        find = {}
        find['ip'] = str_.split()[0]
        find['RFC931'] = str_.split()[1]
        find['user'] = str_.split()[2]
        find['date'] = str_.split('[')[1].split()[0]
        find['gmt'] = str_.split('[')[1].split()[1].strip(']')
        try:
            if 'HTTP' in str_.split('"')[1].split()[-1]:
                find['action'] =  str_.split('"')[1].replace(str_.split('"')[1].split()[2],'').strip()
            else:
                find['action'] =  str_.split('"')[1].strip()
        except:
            find['action'] = '-'
        try:
            find['status'] = str_.split('"')[2].strip().split()[0]
        except:
            find['status'] = '-'
        try:
            find['size'] = str_.split('"')[2].strip().split()[1]
        except:
            find['size'] = '-'
        try:
            find['referrer'] = str_.strip().split('"')[3]
        except:
            find['referrer'] ='-'
        try:
            find['browser'] = str_.strip().split('"')[5]
        except:
            find['browser'] = '-'
        
        return find


    df = pd.DataFrame(list(df[col_name].apply(log_parser).values) )
    if len(df.columns) == len(columns):
        df.columns = columns
    return df
    
    
def XSS_finder(str_):
    flag = 0
    l1 = ['<','>','\\','`']
    l2 = ['/',')','(']
    for char in l1:
        aasci_encoding = '%'+hex(ord(char)).replace('0x','')
        if (char in str_) or (aasci_encoding in str_):
            flag = 1
            break
    if flag != 1:
        try:
            req_str = ''.join(str_.split('?')[1:])
            for char in l2:
                aasci_encoding = '%'+hex(ord(char)).replace('0x','')
                if (char in req_str) or (aasci_encoding in req_str):
                    flag = 1
                    break
        except:
            pass
    return bool(flag)
    
def add_location_data(df,column,keys):
    def loc_dict_maker(unq_series):
        '''
        input:
            series: series of unique ip address.
        output:
            res: dict contain country code, lat, long.
        '''
        def ip_loc(x):
            '''
            input:
                x : ip address
            output:
                dictionary which contain country,latitude and longitude.
            '''
            res = {}
            url = "https://geolocation-db.com/jsonp/"+x
            with urllib.request.urlopen(url) as url:
                data = json.loads(url.read().decode().split("(")[1].strip(")"))
            res = {"country_code":data["country_code"], 'latitude':data['latitude'],'longitude':data['longitude']}
            try:
                res['alpha_3'] = pycountry.countries.get(alpha_2=res["country_code"]).alpha_3
            except:
                res['alpha_3'] = 'Not found'
            return res

        result = {}
        for ip in tqdm(unq_series):
            result[ip] = ip_loc(ip)
        return result
    
    loc_dict = loc_dict_maker(df[column].unique())
    for key in keys:
        df[key] = df[column].apply(lambda x: loc_dict[x][key])
    return df


# In[40]:


def helper(df):
    df = pd.DataFrame(list(df[1].apply(lambda x: x.split())))
    df[3] = df[3].apply(lambda x: x.replace('[','') )
    df[4] = df[4].apply(lambda x: x.replace(']','') )
    df[5] = df.apply(lambda x : ' '.join([x[5],x[6]]).replace('"',''), axis = 1)
    df.drop([1,2,7,6],inplace=True,axis = 1)
    
    return df


# In[41]:


url1 = "https://github.com/ayedaemon/RuckSack-Python/blob/master/log_analysis/access_log"
url2 = "https://github.com/ayedaemon/RuckSack-Python/blob/master/log_analysis/access_log2"
url3 = "https://github.com/robert456456456456/Web_server_log_parser/blob/master/devops.log"
urls=[url1,url2,url3]


# In[42]:


df = html_to_df(urls,clean_empty=True)


# In[43]:


columns = ["User_ID","RFC931","User","date","gmt","action","status","size","referrer","browser"]
df=log_df(df,1,columns)


# In[44]:


df.head()


# # 1.) Country to Which user belongs

# In[45]:


keys = ['country_code', 'latitude', 'longitude', 'alpha_3']
df = add_location_data(df,'User_ID',keys)
df.head()


# In[56]:


from plotly.offline import  iplot


# In[57]:


def geo_plotting(country_series,colorbar_title,title):
    data = dict(
            type = 'choropleth',
            locations = country_series.value_counts().index,
            z = country_series.value_counts().values,
            colorbar = {'title' : colorbar_title},
          )
    layout = dict(
        title = title,
        geo = dict(
                showframe = False,
                projection = {'type':'natural earth'}    
        )
    )
    choromap = go.Figure(data = [data],layout = layout)
    iplot(choromap)


# In[59]:


geo_plotting(df[df['alpha_3']!= 'Not found']['alpha_3'],"Number of Users","User's Geo-location Data")


# # 2. a.) OS Used to Open Webpage

# In[60]:


def OS_dict(browser_series):
    os = ['Windows','Machintosh','Linux','Other']
    res_dict = {o:0 for o in os}
    for browser in browser_series:
        flag = 0
        for i in os[:-1]:
            if i in browser:
                res_dict[i]+=1
                flag = 1
                break
        if flag==0:
            res_dict['Other']+=1
    return res_dict


# In[61]:


OS = OS_dict(df.browser)
px.pie( values=list(OS.values()), names=list(OS.keys()), title='OS Used to open Webpage')


# # 2. b.)Browser Used to Open Webpage

# In[62]:


def brwsr_dict(browser_series):
    brwsr = ['Firefox','Chrome','Opera','Edge','Other']
    res_dict = {o:0 for o in brwsr}
    for browser in browser_series:
        flag = 0
        for i in brwsr[:-1]:
            if i in browser:
                res_dict[i]+=1
                flag = 1
                break
        if flag==0:
            res_dict['Other']+=1
    return res_dict


# In[63]:


BROWSER = brwsr_dict(df.browser)
px.pie( values=list(BROWSER.values()), names=list(BROWSER.keys()), title='Browser Used to open Webpage')


# # 3.) Most and Least Visited Webpages

# In[55]:


def most_least_visited_webpages(df,n=1,m=1):
    result = {}
    result['Most visited webpage'] = list(df['action'].value_counts().head(n).index)
    result['Least visited webpage'] = list(df['action'].value_counts().tail(m).index)
    return result

most_least_visited_webpages(df)


# # 4.) Most and Least Visited Customers

# In[54]:


def most_least_visited_customer(df,n=1,m=1):  
    result = {}
    result['Most visited customer'] = list(df['User_ID'].value_counts().head(n).index)
    result['Least visited customer'] = list(df['User_ID'].value_counts().tail(m).index)
    return result

most_least_visited_customer(df)


# # XSS ATTACK

# In[26]:


df['XSS_attack']=df['action'].apply(XSS_finder)
df


# In[ ]:





# # 5.) Table For XSS Attack Attempts

# In[28]:


df[df['XSS_attack']==True]


# # 6.) Time Series Graph for Hits Vs Time

# In[64]:


df_date = pd.to_datetime(df['date'], format = '%d/%b/%Y:%H:%M:%S')


# In[69]:


def plot_hit_vs_time(df_date,title,xlabel,ylabel):
    layout = go.Layout(height=600, width=1000,title='Hits Vs Time', xaxis=dict(title='Date',color='orange'),
                   yaxis=dict(title='Hits',color='blue'))
    fig = px.line(x=df_date.unique(), y=df_date.value_counts(sort = False))
    fig.layout=layout
    fig.show()

plot_hit_vs_time(df_date,"Hits v/s Time: Complete",'Time',"Hits")


# In[ ]:





# In[ ]:




