
# coding: utf-8

# # Notebook to process the Michigan Claims Loss notes
# ### I used Cause of Loss (COL) = 400 and dates after 2009-12-31
# 

# ### SQL code to generate this file
# #### Step 1
# ```
# drop table if exists kesj.claims_400;
# create table kesj.claims_400 as
#         select distinct
#                 systems_fdwatomcecs.col.rsrv_col_cd,
#                 systems_fdwatomcecs.col.clm_id
#          from
#                 systems_fdwatomcecs.col
#          where
#                 systems_fdwatomcecs.col.rsrv_col_cd ='400';
# ```
# #### Step 2
# ```
# drop table if exists kesj.claims_400_more;
# create table kesj.claims_400_more as
# select distinct 
#         kesj.claims_400.rsrv_col_cd,
#         kesj.claims_400.clm_id,
#         systems_fdwatomcecs.clm.clm_num,
#         systems_fdwatomcecs.clm.clm_st_cd,
# 		systems_fdwatomcecs.clm.st_asgn_st_cd,
# 		systems_fdwatomcecs.clm.los_occr_dt,
# 		systems_fdwatomcecs.clm.los_type_cd,
# 		systems_fdwatomcecs.clm.rpt_agt_st_cd,
# 		systems_fdwatomcecs.clm.los_loc_city_nm,
# 		systems_fdwatomcecs.clm.los_loc_stret_nm,
# 		systems_fdwatomcecs.clm.los_loc_st_cd,
# 		systems_fdwatomcecs.clm.los_loc_desc_txt,
#         systems_fdwatomcecs.clm.los_desc_txt,
#         systems_fdwatomcecs.clm.los_rpt_dt,
#         systems_fdwatomcecs.clm.app_cd,
#         systems_fdwatomcecs.clm.user_type_cd,
# 		systems_fdwatomcecs.clm.loc_qlty_cd,
# 		systems_fdwatomcecs.clm.latud_num,
# 		systems_fdwatomcecs.clm.lngtd_num
# FROM
#         kesj.claims_400
# LEFT OUTER JOIN 
#         systems_fdwatomcecs.clm
# ON
#  (        kesj.claims_400.clm_id = systems_fdwatomcecs.clm.clm_id) ;
#         ```
# #### step 3: 
# ```
# drop table if exists kesj.claims_400_mi;
# create table kesj.claims_400_mi as
# select * 
# FROM
#     kesj.claims_400_more
# WHERE
# 	kesj.claims_400_more.los_loc_st_cd = 'MI' 
# AND kesj.claims_400_more.los_rpt_dt > '2009-12-31' ;
# 
# ```

# ### generic loads

# In[2]:

import os,subprocess
import pandas as pd
import numpy as np
get_ipython().magic(u'matplotlib inline')
#from datetime import datetime
import matplotlib.pyplot as plt
#from sklearn import preprocessing
#from itertools import chain
#import random
plt.style.use('fivethirtyeight') # Good looking plots
#import seaborn as sns


# ### check on the paths

# In[3]:

# check if the local path exists
dir1local = '/home/kesj/work/mi400test1'
if not os.path.exists(dir1local):
    # make the local directory
    get_ipython().system(u'mkdir {dir1local}')

get_ipython().magic(u'pwd')
get_ipython().magic(u'cd {dir1local}')
cwd=os.path.abspath(os.curdir)
print cwd


# In[16]:

def hdfs_path_does_exist(path):
    return subprocess.call(['hdfs','dfs','-ls',path])
    # returns 0 if does_exist; 1 otherwise
    
## function to load into pandas from hdfs (by copying to local filespace)
def pandas_read_hdfs(infile,sep = ';',**kwargs):
    # copy the infile to the cwd
    get_ipython().system(u'hdfs dfs -get {infile} .')
    # identify the local file name
    inname = infile[infile.rfind('/')+1:]
    # read into a data frame
    #if dtype_dict != None:
    #    df = pd.read_csv(inname,sep=sep,**kwargs)
    #else:
    df = pd.read_csv(inname,sep=sep,**kwargs)
    # clean up local filespace
    get_ipython().system(u'rm {inname}')
    return df


# In[9]:

# check if the remote (HDFS) path exists
hdfsdir = '/user/kesj/data/mi400'
if not hdfs_path_does_exist(hdfsdir):
    fnames = get_ipython().getoutput(u'hdfs dfs -ls {hdfsdir}')
    infilenames = [f.split()[-1] for f in fnames[1:]]

print infilenames


# In[17]:

mi400 = pandas_read_hdfs(infilenames[0],header=None)


# In[18]:

len(mi400)


# In[19]:

mi400.head()


# In[20]:

mi400.shape


# In[21]:

mi400_col_names = ['rsrv_col_cd','clm_id','clm_num','clm_st_cd','st_asgn_st_cd','los_occr_dt','los_type_cd','rpt_agt_st_cd',
                   'los_loc_city_nm','los_loc_stret_nm','los_loc_st_cd','los_loc_desc_txt','los_desc_txt','los_rpt_dt',
                   'app_cd','user_type_cd','loc_qlty_cd','latud_num','lngtd_num']
mi400.columns = mi400_col_names
mi400.head()


# In[22]:

len(mi400.los_loc_city_nm.unique())


# ## begin to assess the data
# * how many unique claims?

# In[24]:

len(mi400), len(mi400.clm_num.unique()),len(mi400.clm_id.unique())


# ### why this discrepancy?
# 
