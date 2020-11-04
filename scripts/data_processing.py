import pandas as pd
import numpy as np
from typing import Union, List

def main():
    def lagger(
        dataset:pd.DataFrame, n_lags:int,
        price_columns : Union[str,List[str]]) -> pd.DataFrame:
        """
        Create columns of time lags

        Inputs
        ------
        dataset : dataframe to lag 
        n_lags : number of time lags
        price_columns :
        y_col : target column name(s)
        Returns
        ------
        result : lagged dataframe
        """
        from toolz.curried import reduce
        df = reduce(
            lambda df, lag: df.assign(**{col + '_' +str(lag): dataset[[col]].shift(lag).values for col in price_columns}),
            range(1, n_lags + 1),
            dataset[price_columns])

        result = df.assign(**{col: dataset[col] for col in dataset.drop(price_columns, axis=1).columns}).fillna(0)
        return result[sorted(result.columns)]

    def complete_months(
        df:pd.DataFrame,
        start_date:str,
        end_date:str,
        )->pd.DataFrame:
        """
        Extrapolate the month to 30 day, interpolate new datapoints

        Inputs
        ------
        dataset : dataframe with daily data
        start_date : starting date 
        end_date : ending date 
        Returns
        ------
        df : extrapolated dataframe
        """        
        # TODO: Extend to more support more columns
        
        idx = pd.date_range(start_date,end_date)
        df = daily.reindex(idx)
        df.iloc[:,0]=df.iloc[:,0].interpolate(method='linear').fillna(0)
        
        return df


    def process_data(
        df:pd.DataFrame,freq:str,
        start_date:str='1982-01-01',
        end_date:str='2020-07-01',
        )-> pd.DataFrame:
        """
        Process data to be used by Midas model

        Inputs
        ------
        df : dataframe with data
        start_date : starting date 
        end_date : ending date 
        Returns
        ------
        df : dataframe with the correct format
        """          
        if freq == 'monthly':
            n_lags = 2
        elif freq == 'daily':
            n_lags = 89
            column_name = df.columns[0]
            df = complete_months(df,
                                start_date=start_date,
                                end_date=end_date)
        elif freq == 'quarterly':
            df = df.pct_change().replace([np.inf, -np.inf, np.nan], 0)
            return df.loc[start_date:end_date,:]
            
        else:
            raise NameError('Please choose a frequency (monthly or daily)')
        
        df = df.pct_change().replace([np.inf, -np.inf, np.nan], 0)
        df = lagger(df,n_lags,df.columns).round(2)
        df.index = df.index - pd.Timedelta('1 days')
        df = df.resample('Q', convention='start').asfreq()
        df.index = df.index + pd.Timedelta('1 days')
        
        
        if freq == 'daily': # Make the order right, otherwise 10 will appear before 2
            column_order = [column_name] + [column_name+'_'+str(idx) for idx in range(1,n_lags+1)]
            df = df[column_order]
            
        return df.loc[start_date:end_date,:]
    
    daily = pd.read_csv('../data_raw/daily_data.csv',index_col='Date',parse_dates=True)
    daily_final = process_data(daily,freq='daily')
    monthly = pd.read_csv('../data_raw/monthly_data.csv',index_col='Date',parse_dates=True)
    monthly_final = process_data(monthly,freq='monthly')
    quarterly = pd.read_csv('../data_raw/quarterly_data.csv',index_col='Date',parse_dates=True)
    quarterly_final = process_data(quarterly,freq='quarterly')

    daily_final.to_csv('../data_processed/daily.csv')
    monthly_final.to_csv('../data_processed/monthly.csv')
    quarterly_final.to_csv('../data_processed/quarterly.csv')




if __name__ == "__main__":
    #TODO: Maybe make the reading/writing data files more general
    main()
    