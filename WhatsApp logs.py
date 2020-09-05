#!/usr/bin/env python
# coding: utf-8

# # Getting Users Data from Unofin DB 

# In[1]:


import connectors
import pandas as pd

conn = connectors.db_conn()
cur = conn.cursor()
sql_query = "select name , cast(wa_id as varchar) as wa_id , cast(permission as varchar)as permission from users where wa_id notnull"
db_data = pd.read_sql_query(sql_query, conn)
## Always close the connection
conn = None


# # Reading whatsapp log file

# In[2]:


data = pd.read_csv('chat_logs.txt', sep=r'\\t', engine='python')


# # Creating a column to work with

# In[3]:


data.columns = ["Info"]


# # Text Cleansing

# In[4]:


data = pd.DataFrame(data.Info.str.split(',',1).tolist(), 
                         columns = ['1st','2nd'])


# In[5]:


data = data.drop(['1st'],axis = 1)
data = pd.DataFrame(data['2nd'].str.split('[',1).tolist(), 
                         columns = ['3','4']) 


# In[6]:


data = data.drop(['3'],axis = 1)


# In[7]:


data = pd.DataFrame(data['4'].str.split(']',1).tolist(), 
                         columns = ['5','6']) 


# In[8]:


data['date'] = data['5'].str[:10]


# In[9]:


data['detail'] = data['6'].str[80:]


# In[10]:


data = data.drop(['5'],axis = 1)


# # Getting date of messages

# In[11]:


data['date'] = pd.to_datetime(data['date'])  


# # Setting timeframe

# In[12]:


mask = (data['date'] > '2020-4-1') & (data['date'] <= '2020-7-16')


# In[13]:


data = data.loc[mask]


# In[14]:


data = data.drop(['6'], axis = 1)


# In[15]:


data.head()


# # Segregating wa_id from text

# In[16]:


data = data[data.detail.str.contains('wa_id',case=False)]


# In[17]:


data = pd.DataFrame(data.detail.str.split(',',1).tolist(), 
                         columns = ['noise','sound'])


# In[18]:


data = data.drop(['noise'], axis = 1)


# In[19]:


data = pd.DataFrame(data.sound.str.split('>',1).tolist(), 
                         columns = ['shit','impshit'])


# In[20]:


data = data.drop(['shit'], axis = 1)


# In[21]:


data = pd.DataFrame(data.impshit.str.split('}',1).tolist(), 
                         columns = ['wa_id','nut'])


# In[22]:


data = data.drop(['nut'], axis = 1)
data.head()


# In[23]:


data['wa_id'] = data['wa_id'].str.replace('"' , '')


# # Adding dummy rows for name and permission

# In[24]:


data['length']= data['wa_id'].str.len() 


# # Handling Abrations

# In[25]:


if (data['length'] > 12).any():
    data['wa_id'] = data['wa_id'].str[:12]
else:
    data['wa_id'] = data['wa_id']


# In[26]:


data = data.drop(['length'],axis = 1)


# In[27]:


data = data[data['wa_id'].str.startswith('91')]


# In[28]:


print(data)


# # Merging DB Data and Logs Data

# In[29]:


merged_data = pd.merge(left=data, right=db_data, how = 'left', left_on='wa_id', right_on='wa_id')


# In[30]:


print(merged_data)


# # Selecting unique numbers

# In[31]:


data = data.drop_duplicates()
merged_data = merged_data.drop_duplicates()


# In[32]:


print(data)


# In[33]:


print(merged_data)


# In[34]:


output = merged_data.to_csv('whatsapp_data.csv')

