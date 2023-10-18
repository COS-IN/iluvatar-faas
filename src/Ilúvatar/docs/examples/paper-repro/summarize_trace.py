import pandas as pd
import numpy as np 

## Class AnalyzeTrace
### provides pure analysis functions 
    # function that generates a series of inter arvial times based on input series of start times
    # calculate average iat based on series of iats 
    # calculate number of running functions - average arival rate * average execution time 
class AnalyzeTrace:

    def __init__(self):
        pass

    def gen_iat( self, d: pd.Series ) -> pd.Series:
        return d.diff().dropna()
    
    def avg_iat( self, d: pd.Series ) -> float:
        return float( d.mean() )
    
    def count_littles_law( self, avg_iat, avg_execution_time ) -> float:
        return float(1.0/avg_iat)*float(avg_execution_time)
    
    def trim_iats( self, iats ):
        iats = iats / 1000.0
        #iats = iats[ iats < (60*3) ]
        return iats
    
    def get_cdf( self, iats, duration_s ) -> np.array:
        f,bins = np.histogram( iats.to_numpy(), range=(0,duration_s), bins=duration_s, density=True ) 
        cf = np.cumsum( f )
        return cf

    def plot_iat_cdf( self, iats, duration_s, args={} ) -> str:
        return iats.hist( bins=duration_s, histtype='step', cumulative=True, density=True,  **args )

    def plot_iat_density( self, iats, duration_s, args={} ) -> str:
        return iats.plot.kde(**args)
