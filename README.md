# yam-dash

Early version is publicly accessible at https://heroku-dash.herokuapp.com/.

Deployed at http://yamdash.com. Privacy policy at http://yamdash.com/static.

You must added by me to the access list, but you can check it out if you are interested.

A basic, but fast, interactive dashboard for visualizing and analyzing financial and economic data from APIs including Quandl and Fred. 

---

Tech:
  * Python (Pandas, PyMongo, JSON, + )
  * Flask
  * Dash (from Plotly)
  * Google Auth
  * MongoDB
  * React
  * nginx
  

#### Note: This does not work as-is. It needs a MongoDB instance with a db `db1` and collections `quandl_collection` and `fred_collection` running on the same local server instance. In addition, you'll need secret keys for Flask and API keys for FRED and Quandl.
