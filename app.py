# module imports 
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go
from numpy import random
import pandas as pd
from datetime import date,timedelta,datetime as dt
from flask import Flask,render_template
from dash_google_auth import GoogleOAuth 
from flask_pymongo import PyMongo
import os
from werkzeug.contrib.fixers import ProxyFix
import base64
import quandl
from fredapi import Fred
import json
import multiprocessing.pool
import functools
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import selenium.webdriver.chrome.service as service


# file imports
# from API_KEYS import *
# from etl import *

# configure app & google auth stuff
server = Flask(__name__)
server.wsgi_app = ProxyFix(server.wsgi_app)

app = dash.Dash(
    __name__,
    server=server,
    url_base_pathname='/',
    auth='auth'
)

authorized_emails=[
    'yamdashcom@gmail.com'
]
auth = GoogleOAuth(
    app,
    authorized_emails,
)
@server.route("/")
def MyDashApp():
    return app.index()
@server.route("/static")
def hello():
    message = "This app does not collect or store user data past using your email to login. We do not track your web activity."
    return render_template('index.html',message=message)
# configure google oauth using environment variables
server.secret_key = os.environ.get("key", "secret")
#server.config["GOOGLE_OAUTH_CLIENT_ID"] = os.environ["GOOGLE_OAUTH_CLIENT_ID"]
#server.config["GOOGLE_OAUTH_CLIENT_SECRET"] = os.environ["GOOGLE_OAUTH_CLIENT_SECRET"]

# LOCAL USE ONLY -- allow for insecure transport for local testing (remove in prod)
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
#os.environ['OAUTHLIB_RELAX_TOKEN_SCOPE']=1

yam_path = 'yam.png'
encoded_yam = base64.b64encode(open(yam_path,'rb').read())

uri = "mongodb://localhost:27017"
mongo = PyMongo(server,uri)

#server.config['MONGO_HOST'] = 'localhost'
#server.config['MONGO_PORT'] = 27017
#server.config['MONGO_DB'] = 'db1'
#client = MongoClient()
db = mongo.cx.db1
#print(db.fred_collection.find_one({"symbol":"GDPC1"})['name'])

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
            return "No updates for symbol "+symbol+" on "+" at "+str(dt.now())
        
        df0 = pull_data(data_source=data_source,symbol=symbol)
        df = pd.concat([df0,df],axis=0)
        collection.update_one(
            {"symbol":symbol},
            {
                "$set":{
                            "end_date":df.index.max(),
                            "updated_on":dt.now(),
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
            return "No updates for symbol "+symbol+" on "+" at "+str(dt.now())
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
@timeout(5)
def pull_quandl(target,start_date="",end_date=""):
    while True:
        if True:
            return quandl.get(target,start_date=start_date,end_date=end_date)
def quandl_key(key):
    quandl.ApiConfig.api_key = key

quandl_key(QUANDL_API_KEY)


# multiple value dropdown
# date range input
# stored by State by clicking submit button
app.layout = html.Div([
    html.Div([
        html.Img(
            src='data:image/png;base64,{}'.format(encoded_yam.decode()),
            style=dict(aflt='yam',height='60',display='inline-block')
        ),        
        html.H1(
            ' Yam Dash',
            style=dict(fontFamily='helvetica',display='inline-block')
        ),
        html.H3(
            '  Sweet, starchy insights',
            style=dict(fontFamily='helvetica',display='inline-block',color='brown')
        )
    ],style={"border-style":'solid',"background-color":"bisque","height":'60',"padding":'5px'}),
    # stock graph
    html.Div([
        dcc.Graph(
            id='stock-graph',
            figure=dict( # initial figure object
                data=[
                    go.Scatter(
                        x=[x for x in range(50)],
                        y=[x**0.5 for x in range(0,500,10)],
                        mode='lines'
                    )
                ],
                layout=go.Layout(
                    title='Graph of Arbitrary Data'),
                    showlegend=True,
                    legend=go.layout.Legend(
                        x=.5,
                        y=0
                    )                )
            )
    ],style=dict(width='95%')),
    html.Br(),
    html.Hr(),
    html.Br(),
    # selection options
    html.Div([
        html.Div([
            html.Div([
                # stock selector            
                html.H3('Stock symbol'),
                dcc.Dropdown(
                    id='stock-dropdown',
                    options=[{"label":x['name'], "value":x['symbol']} for x in db.quandl_collection.find({})],
                    value=[],
                    multi=True,
                ),
                html.Button(
                    children='Refresh Stocks (click this if you added symbol(s) from Quandl)',
                    id='refresh-stocks-dropdown-button',
                    n_clicks=0
                )
            ],style={"width":'33%',"display":'inline-block',"vertical-align":'top'}),
            html.Div([
                # economic measure selector            
                html.H3('Economic Measures (Rates/Percents)'),
                dcc.Dropdown(
                    id='economic-measures-dropdown',
                    options=[{"label":x['name'], "value":x['symbol']} for x in db.fred_collection.find({})],
                    value=[],
                    multi=True,
                ),
                html.Button(
                    children='Refresh Measures (click this if you added symbol(s) from FRED)',
                    id='refresh-measures-dropdown-button',
                    n_clicks=0
                )
            ],style={"width":'33%',"display":'inline-block',"vertical-align":'top'}),

            html.Div([
                # economic measure selector            
                html.H3('Economic Measures (Non-Rates/Percents)'),
                dcc.Dropdown(
                    id='big-economic-measures-dropdown',
                    options=[{"label":x['name'], "value":x['symbol']} for x in db.fred_collection.find({})],
                    value=[],
                    multi=True,
                )
            ],style={"width":'33%',"display":'inline-block',"vertical-align":'top'}),
        ]),
        
        html.Div([
            # date input boxes
            html.Div([
                html.Div([
                    html.H4('Start Date:'),
                    dcc.Input(
                        id='start-date-input',
                        type='text',
                        value='',
                        style=dict(fontSize='15px')
                    )
                ],style=dict(display='inline-block')),
                html.Div([
                    html.H4('End Date:'),
                    dcc.Input(
                        id='end-date-input',
                        type='text',
                        value='',
                        style=dict(fontSize='15px')
                    )
                ],style=dict(display='inline-block'))
            ])
        ]),
        # index radio
        html.Br(),
        html.Div([
            dcc.RadioItems(
                id='radio-index',
                options=[
                    dict(label='Price Level',value='price'),
                    dict(label='Price Index',value='index')
                ],
                labelStyle=dict(display='inline-block',),
                value='price'
            )
        ])
    ]),
    # submit button
    html.Br(),
    html.Div([
        html.Button(
            children='Submit Graph Options',
            id='submit-button',
            n_clicks=0,
            style=dict(fontSize='20px')
        )
    ],style=dict(display='inline')),


    html.Br(),
    html.Br(),
    html.Hr(),
    html.H3('Input new data'),
    html.Br(),
    html.Div([
        html.Div([
            html.Div(id='submit-input-output-container',children=['Enter your values to submit.'],style=dict(height='30')),
            html.Div([
                dcc.Input(id='symbol-input-box',type='text',style=dict(width='400',fontSize='15px',border='solid 1px blue')),
                html.Button('Submit Input',id='symbol-input-button',n_clicks=0,style=dict(fontSize='15px'))
            ])
        ]),
        # div to display error/success messages
        html.Div(
            id='readout-div',
            children=['System messages will display here.'],
            style={"overflow-y":'scroll',"height":'150',"width":"400","border":"solid 2px black","fontFamily":"monospace"}
        ),
    ],style=dict(display='inline')),
    
    # hidden div for dropdown options
    html.Div(
        id='test-intermediate-div',
        style={"display":"none"}
    ),

    html.Br(),
    html.Br(),
    html.Hr(),
    html.Br(),
    dcc.Markdown('''
# Yam Dash v0.0.1
## User Guide 

Yam Dash is a simple, useful tool to interactively view data from APIs of Quandl and FRED (so far).

### Using the dashboard

The dashboard has a simple layout: a graph section, the selection inputs, and the data input section.

---

### Graph Section

**Dropdowns**

There are three dropdowns to select data: 
* Stocks
* Economic Measures (rates and percents)
    * These are graphed on a separate axis to be able to visualize small percentage changes on the same graph as stock prices and economic measures.
* Economic Measures (non-rates and percents)
    * Measures selected in this dropdown are graphed on the same axis as the stocks and can be indexed as well.

These dropdowns can be typed in to search for stocks by name or symbol as well as scrolling down the options.

The dropdowns for the economic measures actually contain the same options - so if you'd like a non-rate/percent economic measure graphed on a separate axis, you can just select it in the middle dropdown - but you wouldn't be able to see rate/percent economic measures at the same time. It's a tradeof.

**Date input**

Date input is typed. It is flexibly interpreted - i.e. any of the following inputs will be interpreted as you mean it. It requires specificity with decreasing granularity, meaning if there is a day specified, you must also include a month and year. In short, if you can read it, the computer will understand it.
* Jan 2009 = 01/01/2009
* 13 Feb 2007 = 02/13/2007
* 2007 = 01/01/2009
* 1/1/07 = 01/01/2007
* November 17, 2008 = 11/17/2008

The format and specificity can be different in each cell.

Leaving each box empty pulls the data from the earliest existing point for each symbol up to today or to the latest available date.

**Price Level vs. Price Index selector**

This determines whether or not the stock price data and non-rate economic measures are 100-indexed or not. 

However, indexing can also create problems when comparing stable economic measures to volatile stocks - insight is limited when graphed on the same axis. Use the middle dropdown instead in these situatations. 

Default value is price level.

**"Submit Graph Options" button** 

Clicking this button sends the input from the symbols, dates, and index selector. 

### Input section

This section allows you to input your own API codes to pull that data into the system.

**Input Box**

The input is a comma-separated list of sources and API codes, with no spaces (though spaces won't mess it up). The system checks whether each symbol is a source name, and if it is, adds the data and metadata from each API code to the database until it encounters another source code or the input ends.

Adding the data is triggered by clicking the "Submit Input" button.

Sources (case insensitive):
* quandl 
* fred

Example input: 

`quandl,WIKI/TSLA, WIKI/KO,fred,GDPC1,M12MTVUSM227NFWA`

> This would add Tesla and Coca-Cola from Quandl and Real GDP and Vehicle Miles Traveled from FRED to the database.

**System readout box**

This box is where the system readout prints out when:
* Adding each symbol was successful,
* Adding each symbol was unsuccessful,
* The symbol already exists in the system. 

**Updating the symbol selection dropdowns**

After adding data, update the dropdowns by clicking the "Refresh Stocks" button, "Refresh Measures" button, or both, depending on if you added from FRED or from Quandl or both. *The symbols will not be accissble from the dropdowns until you click these buttons.*

### Current known problems or limitations

* The ability to save and load the session state exists but does not yet in deployment. Removing user functionality would allow this to work.
* User functionality do not work in deployment yet, only login
* Quandl data is limited to the free WIKI database or other datasets with a "close" column - which is not updated as of 03/27/2018 for most of the stocks. Stock prices are nominal - they do not deal with stock splits. See AAPL in 2014 for example. Just useful for POC. Will need premium datastream from EOD or use a different stock data provider.
    * Because it's not updated, the metadata does not exist to scrape from the Quandl site like actively updated datasets. This means the names are just the symbols instead of the company names. Will be easily changeable with a better data stream.
    * To fix, need to add a column selector for non-stock data (e.g. CME) that can move functionality past the "close" column of the WIKI data.

''',
    containerProps=dict(
        style=dict(width='50%')
    ))

])





#-------------------------------------------------------------------------------------------------------------




@app.callback(
    Output('test-intermediate-div','children'),
    [Input('symbol-input-button','n_clicks')],
    [State('symbol-input-box','value')])
def input_symbols(n_clicks,s):
    if n_clicks==0:
        return[]
    symbols = [x.strip() for x in s.split(',')]
    readout = []
    if symbols[0].lower()in ['quandl','fred']:
        source = symbols[0].lower()
    else:
        readout.append("Incorrect data source: {}".format(symbols[0]))
        return json.dumps({"readout":readout})
    #
    for sym in symbols:
        if sym.lower() in ['quandl','fred']:
            source = sym.lower()
            continue
        # check if the symbol already exists
        # if symbol not in database, initialize the symbol
        if source == 'quandl':
            if sym in [x['data-symbol'] for x in db.quandl_collection.find({})]:
                readout.append("Already available: {}".format(sym))
                continue
            try:
                initialize_symbol('quandl',sym.split('/')[1],sym)
                readout.append("{} successfully added.".format(sym))
            except BaseException as e:
                readout.append('{} could not be added.'.format(sym))
                print(str(e))
        if source == 'fred':
            if sym in [x['symbol'] for x in db.fred_collection.find({})]:
                readout.append("Already available: {}".format(sym))
                continue
            try:
                initialize_symbol('fred',sym)
                readout.append("{} successfully added.".format(sym))
            except:
                readout.append('{} could not be added.'.format(sym))
    return json.dumps({"readout":readout})

@app.callback(Output('readout-div','children'),
            [Input('test-intermediate-div','children')],
            [State('symbol-input-button','n_clicks')])
def send_readout(readout,n_clicks):
    if n_clicks==0:
        return "System messages will display here."
    readout=json.loads(readout)['readout']
    return_str = ""
    for line in readout :
        return_str += line + '\n'
    return return_str

@app.callback( # send traces to graph object
    Output('stock-graph','figure'),
    [Input('submit-button','n_clicks')],
    [State('stock-dropdown','value'), # stocks/indexes in quandl
     State('economic-measures-dropdown','value'), # economic measures in fred
     State('big-economic-measures-dropdown','value'),
     State('start-date-input','value'),  # start date
     State('end-date-input','value'), # end date
     State('radio-index','value')])
def graph_callback(n_clicks,stocks,measures,big_measures,start,end,index_level):
    if n_clicks==0:
        return dict( # initial figure object
                    data=[
                        go.Scatter(
                            x=[x for x in range(50)],
                            y=[x**0.5 for x in range(0,500,10)],
                            mode='lines'
                        )
                    ],
                    layout=go.Layout(
                        title='Graph of Arbitrary Data'),
                        showlegend=True,
                        legend=go.layout.Legend(
                            x=.5,
                            y=-.15
                        ),
                        margin=go.layout.Margin(l=20,r=20,t=40,b=40),
                        height=400
                    )
    if start: 
        start = pd.to_datetime(start)
    if end:
        end = pd.to_datetime(end)
    graph_title='Graph of '
    for stock in stocks:
        graph_title += stock +', '
    for measure in measures:
        graph_title += measure +', '
    for measure in big_measures:
        graph_title += measure +', '
    fig = dict(
        data=[]
    )
    for stock in stocks:
        df = pull_data('quandl',stock)
        df.columns = [c.lower() for c in df.columns]
        if start:
            df = df.loc[start:]
        if end:
            df = df.loc[:end]
        if index_level=='index':
            df['close'] = [x/df['close'][0]*100 for x in df['close']]
        fig['data'].append(
            go.Scatter(
                x=df.index,
                y=df['close'],
                mode='lines',
                name=stock
            )
        )
    for measure in measures:
        df = pull_data('fred',measure)
        if start:
            df = df.loc[start:]
        if end:
            df = df.loc[:end]
        fig['data'].append(
            go.Scatter(
                x=df.index,
                y=df[df.columns[0]],
                mode='lines',
                name=db.fred_collection.find_one({"symbol":measure})['name'],
                yaxis='y2'
            )
        )
    for measure in big_measures:
        df = pull_data('fred',measure)
        if start:
            df = df.loc[start:]
        if end:
            df = df.loc[:end]
        if index_level=='index':
            df[df.columns[0]] = [x/df[df.columns[0]][0]*100 for x in df[df.columns[0]]]
        fig['data'].append(
            go.Scatter(
                x=df.index,
                y=df[df.columns[0]],
                mode='lines',
                name=db.fred_collection.find_one({"symbol":measure})['name'],
            )
        )
    fig['layout']=go.Layout(
        title=graph_title,
        height=500,
        yaxis=dict(title="Stock Prices"),
        yaxis2 = dict(overlaying='y',side='right',position=.99,title='Economic Measures'),
        showlegend=True,
        legend=dict(orientation="h")
        #hovermode='closest'
    )


    return fig

@app.callback(Output('economic-measures-dropdown','options'),
            [Input('refresh-measures-dropdown-button','n_clicks')])
def update_measures_dropdown(n_clicks):
    return [{"label":x['name'], "value":x['symbol']} for x in db.fred_collection.find({})]
@app.callback(Output('big-economic-measures-dropdown','options'),
            [Input('refresh-measures-dropdown-button','n_clicks')])
def update_big_measures_dropdown(n_clicks):
    return [{"label":x['name'], "value":x['symbol']} for x in db.fred_collection.find({})]

@app.callback(Output('stock-dropdown','options'),
            [Input('refresh-stocks-dropdown-button','n_clicks')])
def update_stocks_dropdown(n_clicks):
    return [{"label":x['name'], "value":x['symbol']} for x in db.quandl_collection.find({})]





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






if __name__ == '__main__':
    app.run_server(threaded=True, debug=True)

