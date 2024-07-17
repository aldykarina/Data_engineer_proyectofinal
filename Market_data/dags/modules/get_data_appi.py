import requests
import pandas as pd
import datetime
import ccxt
from io import StringIO
import logging

logging.basicConfig(
    filename='app.log',
    filemode='a',
    format='%(asctime)s ::GetDataModule-> %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO)
    

class DataInformation:
    def __init__(self, function:str = "TIME_SERIES_DAILY", symbols:list = ['SPY', 'AAPL', 'GOOGL'], API_KEY:str = None, cryptos:list = ['BTC/USD', 'ETH/USD']) -> None:
        self.function = function
        self.symbols = symbols
        self.API_KEY = API_KEY
        self.cryptos = cryptos
    
    def get_data_1(self, symbol, API_KEY):
        url = f'https://www.alphavantage.co/query?function={self.function}&symbol={symbol}&apikey={API_KEY}&outputsize=compact'
        response = requests.get(url)
        data = response.json()
        
        time_series = data['Time Series (Daily)']
        df = pd.DataFrame.from_dict(time_series, orient='index')
        df = df.rename(columns={
            '1. open': 'open_price',
            '2. high': 'high_price',
            '3. low': 'low_price',
            '4. close': 'close_price',
            '5. volume': 'volume'
        })
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        df = df.reset_index().rename(columns={'index': 'date'})
        df['symbol'] = symbol
        df['ingestion_time'] = datetime.datetime.now() 
        
        # Crear columna ID como clave primaria compuesta
        df['ID'] = df['date'].astype(str) + '_' + df['symbol']
        
        # Reorganizar las columnas 
        cols = ['ID'] + [col for col in df.columns if col != 'ID']
        df_s = df[cols]
              
        return df_s
          
     
    def get_data_2(self, cryptos):
        
        # Calcular la fecha de inicio (hace 90 días)
        start_date = datetime.datetime.now() - datetime.timedelta(days=90)
        since = int(start_date.timestamp() * 1000)
  
        data = []
        # Función para obtener datos históricos
        exchange = ccxt.kraken()
        
        def fetch_ohlcv(symbol, since, limit=100):
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1d', since=since, limit=limit)
            return ohlcv

        # Obtener los datos de cada criptomoneda
        for crypto in cryptos:
            ohlcv = fetch_ohlcv(crypto, since)
            for entry in ohlcv:
                data.append([
                    datetime.datetime.fromtimestamp(entry[0] / 1000).strftime('%Y-%m-%d'),
                    entry[1],  # open_price
                    entry[2],  # high_price
                    entry[3],  # low_price
                    entry[4],  # close_price
                    entry[5],  # volume
                    crypto.split('/')[0]  # symbol
                ])

        # Crear un DataFrame
        df_c = pd.DataFrame(data, columns=['date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 'symbol'])
        df_c['ID'] = df_c['date'].astype(str) + '_' + df_c['symbol']
        df_c['ingestion_time'] = datetime.datetime.now() 
        # Reorganizar las columnas 
        cols = ['ID'] + [col for col in df_c.columns if col != 'ID']
        df_c = df_c[cols]
        
        return df_c 
            
          
    def get_all_data(self, API_KEY):
        
        all_data = []
        for symbol in self.symbols:
            df = self.get_data_1(symbol, API_KEY)
            if not df.empty:
                all_data.append(df)
                
        df_from_data_2 = self.get_data_2(self.cryptos)
        
            
        try:
                # Concatenar todos los DataFrames, incluido el de get_data_2
            if all_data:
                combined_data = pd.concat(all_data + [df_from_data_2], ignore_index=True)
            else:
                combined_data = df_from_data_2
            
            # Reemplazar valores nulos o vacíos
            combined_data.fillna(0, inplace=True)
            # Evitar datos duplicados
            combined_data.drop_duplicates(subset=['ID'], inplace=True)
            print(combined_data)       
           
            buffer = StringIO()
            combined_data.info(buf=buffer)
            s = buffer.getvalue()
            logging.info(s)
            logging.info(f"Data created")
            return combined_data
        
        except Exception as e:
            logging.error(f"Not able to import the data from the api\n{e}")                  
            raise