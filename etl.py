"""
script for recurring use to initialize and add to the existing data
"""
# module imports
import pandas as pd
from fredapi import Fred
import quandl
from pymongo import MongoClient
import json
import time
from datetime import timedelta,date,datetime
import multiprocessing.pool
import functools
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import selenium.webdriver.chrome.service as service

# file imports
from API_KEYS import *




def quandl_key(key):
    quandl.ApiConfig.api_key = key


client = MongoClient()
db = client.test_db

#############################
# DB_OBJ CLASS
#############################
class db_obj :    
    """
    a) JSON of db objects that depend on data source
    b) functions to choose data structure and initialize data
    Only useful when initialized in an environment containing an open PyMongo client connection called "db" with two collections: fred_collection and quandl_collection
    """
    import pandas as pd
    import pandas_datareader as pddr
    from fredapi import Fred
    import quandl
    from pymongo import MongoClient
    import json
    import time
    from datetime import timedelta,date,datetime
    import multiprocessing.pool
    import functools
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    import selenium.webdriver.chrome.service as service

    def __init__(self,data_source,symbol_name,data_id="") :
        self.data_source = data_source
        self.symbol_name = symbol_name
        if data_id == "" :
            self.data_id = symbol_name
        else:
            self.data_id = data_id
        self.db_obj = {}
        self.timeout_len = 5
        
        if self.data_source == 'quandl':
            self.collection = db.quandl_collection
        if self.data_source == 'fred':
            self.collection = db.fred_collection

    # create correct json structure for the source
    def set_data_structure(self) :
        if self.data_source=='quandl':
            self.db_obj = {
                                'symbol':self.symbol_name,
                                'data-symbol':self.data_id,
                                "name":"",
                                "data":{
                                    #"daily_data":{},
                                    #"weekly_data":{},
                                    #"monthly_data":{}
                                },
                                "metadata":{},
                                "start_date":"",
                                "end_date":""
                        }
        if self.data_source=='fred':
            self.db_obj = {
                                'symbol':self.symbol_name,
                                'data-symbol':self.data_id,
                                "name":"",
                                "data":{},
                                "metadata":{
                                    "units":"",
                                    "frequency":"",
                                    "url":"",
                                    "id":"",
                                    "citation":""
                                },
                                "start_date":"",
                                "end_date":"",

                            }            
    ## get data 
    def get_data(self) :
        if self.data_source == 'quandl' :
            self.db_obj['data'] = pull_quandl(self.data_id)
            self.db_obj['data'].sort_index(inplace=True)
            self.db_obj['start_date']=self.db_obj['data'].index.min()
            self.db_obj['end_date']=self.db_obj['data'].index.max()
            self.db_obj['data'] = self.db_obj['data'].to_json(date_unit='s')
        if self.data_source == 'fred' :
            freddy = Fred(api_key=FRED_API_KEY)
            self.db_obj['data'] = pd.DataFrame(freddy.get_series(self.data_id),columns=[self.symbol_name])
            self.db_obj['data'].sort_index(inplace=True)
            self.db_obj['start_date']=self.db_obj['data'].index.min()
            self.db_obj['end_date']=self.db_obj['data'].index.max()
            self.db_obj['data'] = self.db_obj['data'].to_json(date_unit='s')
    
    # get metadata
    def scrape(self) :
        name=self.symbol_name
        metadata={}
        if self.data_source == 'quandl':
            if "WIKI/" in self.data_id:
                pass
            else:
                try:
                    chrome_options = Options()
                    chrome_options.add_argument("--headless")
                    driver = webdriver.Chrome(options=chrome_options)
                    driver.get("https://www.quandl.com/data/"+self.data_id)
                    metadata = dict(
                        frequency=driver.find_element_by_class_name("dataset-frequency").text,
                        url=driver.current_url,
                    )
                    name=driver.find_element_by_class_name('dataset__title').get_attribute('title')
                    driver.quit()
                except BaseException as e:
                    print('Quandl scrape failed: '+name+"\n----Error:----\n"+str(e))

        if self.data_source == 'fred' :
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            driver = webdriver.Chrome(options=chrome_options)
            driver.get("https://fred.stlouisfed.org/series/"+self.data_id)
            metadata = dict(
                frequency = driver.find_element_by_class_name("series-meta-value-frequency").get_attribute('innerHTML').replace("\n","").strip(),
                url = driver.current_url,
                units = driver.find_element_by_class_name('series-meta-value-units').find_element_by_xpath("./..").text.replace('\n'," "),
                source = driver.find_element_by_class_name("series-source").get_attribute('href')
            )
            name = driver.find_element_by_id("series-title-text-container").text.split("(")[0].strip()
            driver.quit()
        self.db_obj['metadata'] = metadata
        self.db_obj['name'] = name
        del name,metadata

    # store the data in MongoDB        
    def store_data(self) :
        self.collection.insert_one(self.db_obj)


#############################    

















#############################
# ETL FUNCTIONS
#############################

def initialize_symbol(data_source,symbol,data_id=""):
    """
    single function to initialize data based on db_obj class
    """
    if data_source == 'quandl':
        collection = db.quandl_collection
    if data_source == 'fred':
        collection = db.fred_collection
 
    if collection.count_documents({'symbol':symbol}) >0:
        return "Already exists in database: "+symbol+'.'
    #try:
    obj = db_obj(data_source,symbol,data_id)
    obj.set_data_structure()
    obj.get_data()
    obj.scrape()
    obj.store_data()
    #except BaseException as e:
    #    return'n\Pulling data failed: '+data_source+" -- "+data_id+'\nError:\n'+str(e)+'\n')





def pull_data(data_source,symbol) :
    df = pd.DataFrame(dict(Close=[]))
    if data_source == 'fred':
        collection = db.fred_collection
        try:
            df = pd.DataFrame(json.loads(collection.find_one({"symbol":symbol})['data']))
        except:
            pass

    if data_source == 'quandl':
        collection = db.quandl_collection
        try:
            df = pd.DataFrame(json.loads(collection.find_one({"symbol":symbol})['data']))
        except:
            pass
    
    df.index = pd.to_datetime(df.index,unit='s')
    df.sort_index(inplace=True)
    
    return df






def update_data(data_source,symbol) :
    """
    update data for any of the available data sources
    """
    if data_source == 'quandl' :
        collection = db.quandl_collection
        if collection.count_documents({"symbol":symbol}) == 0:
            return "Symbol does not exist in database: "+symbol+'.'
        df = pull_quandl(target=collection.find_one({"symbol":symbol})['data-symbol'],
                        start_date=str(collection.find_one({"symbol":symbol})['end_date']+timedelta(1))[:10])
        df.sort_index(inplace=True)
        if df.empty:
            return "No updates for symbol "+symbol+" on "+" at "+str(datetime.now())
        
        df0 = pull_data(data_source=data_source,symbol=symbol)
        df = pd.concat([df0,df],axis=0)
        collection.update_one(
            {"symbol":symbol},
            {
                "$set":{
                            "end_date":df.index.max(),
                            "updated_on":datetime.now(),
                            "data":df.to_json(date_unit='s')
                        }
            }
        )
        del df,df0
        
    if data_source == 'fred':
        collection = db.fred_collection
        if collection.count_documents({"symbol":symbol}) == 0:
            return "Symbol does not exist in database: "+symbol+'.'
        freddy = Fred(api_key="9719702db51a54b30af186dee41a6aac")
        df = pd.DataFrame(freddy.get_series(symbol),columns=[symbol])
        df.sort_index(inplace=True)
        if df.empty:
            del df,freddy
            return "No updates for symbol "+symbol+" on "+" at "+str(datetime.now())
        collection.update_one(
            {"symbol":symbol},
            {"$set":
                 {"end_date":df.index.max(),
                  "data":df.to_json(date_unit='s')}
            }
        )
        del df,freddy





def timeout(max_timeout=60):
    """Timeout decorator, parameter in seconds."""
    def timeout_decorator(item):
        """Wrap the original function."""
        @functools.wraps(item)
        def func_wrapper(*args, **kwargs):
            """Closure for function."""
            pool = multiprocessing.pool.ThreadPool(processes=1)
            async_result = pool.apply_async(item, args, kwargs)
            # raises a TimeoutError if execution exceeds max_timeout
            return async_result.get(max_timeout)
        return func_wrapper
    return timeout_decorator






timeout(5)
def pull_quandl(target,start_date="",end_date=""):
    while True:
        if True:
            return quandl.get(target,start_date=start_date,end_date=end_date)





