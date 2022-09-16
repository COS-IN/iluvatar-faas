"""
This script takes in the battery.log and writes the plot to a file.

Input: 
    - battery.log
Output:
    - battery.jpg
"""

import argparse 
import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt
from datetime import datetime
from datetime import time
from tabulate import tabulate

argparser = argparse.ArgumentParser()
argparser.add_argument( "--log", '-l', help="Battery log file", required=True,
                      type=str)
argparser.add_argument( "--pck", '-p', help="Combined Energy Pickle File", required=True,
                      type=str)

args = argparser.parse_args() 


tracing = False

def trace( msg ):
    if tracing:
        print(msg)

def read_log( path ):

    cols = ["timestamp", "energy_mj"]
    df = pd.read_csv( path )
    df.columns = cols

    df['abs_time'] = [ unix_time_millis( datetime.fromisoformat(df['timestamp'][i]) ) for i in range(len(df)) ]
    
    return df

def read_pckl( path ):

    df = pd.read_pickle( path )

    return df

def timediff( df, s, e ):
    
    start = df['timestamp'][s]
    delta = df['timestamp'][e] - start
    return int( delta.total_seconds() )

epoch = datetime.fromisoformat("2020-09-02 11:16:42-04:00")
def unix_time_millis(dt):
    trace(epoch)
    trace(dt)
    return (dt - epoch).total_seconds()

def getderivative( df, col ):
    
    # deriv = [ df[col][i-1] - df[col][i] for i in range(1,len(df)) ]
    # deriv.insert( 0, 0 )
    
    deriv = -np.gradient( df[col] )
    
    old_val = 0
    
    """
    for i in range(len(deriv)):
        if deriv[i] == 0:
            deriv[i] = old_val
        else:
            old_val = deriv[i]
    """
    
    deriv = pd.DataFrame(deriv).ewm(span=20).mean()
    return deriv


if __name__ == "__main__":

    func_fact = 100

    df = read_log( args.log )
    trace( df )

    # take the energy list and plot it using matplot 
    trace( df['energy_mj'][0:] )
    
    deriv = getderivative( df, 'energy_mj' )
    trace( deriv )

    df['power']=deriv
    
    cdf = read_pckl( args.pck )
    
    # conver datetime to seconds 
    start = cdf['timestamp'][0]

    func = []
    abstime = []
    abstime_base = unix_time_millis( cdf['timestamp'][0] )
    for i in range(len(cdf)):
        if len(cdf['cumulative_running'][i]) == 0:
            func.append( 0 )
        else:
            func.append( len(cdf['cumulative_running'][i] * func_fact) )
        
        abstime.append( unix_time_millis(cdf['timestamp'][i]) )
            

    cdf['funcs_tally'] = func
    cdf['abs_time'] = abstime
    
    start = abstime[0]
    end = abstime[-1]

    full_df = df.join( cdf.set_index('abs_time'), on='abs_time', how='inner', lsuffix='_l', rsuffix='_r')
    #full_df = cdf.join( match, lsuffix='_l', rsuffix='_r' )
    
    with open("full_table.txt", 'w') as f:
        f.write(tabulate(full_df, headers='keys', tablefmt='psql'))
    #### Determining correlation 
    # Set window size to compute moving window synchrony.
    r_window_size = 15

    plt.close()
    for i in range(1,3):
        rsize = 30 * i *3

        # Compute rolling window synchrony
        rolling_r = full_df['power'].rolling(window=rsize, center=True).corr(full_df['funcs_tally'])
        plt.plot( rolling_r[10:], label=f"rsize: {rsize}" )
        plt.legend()
    plt.ylabel(f"Pearson correlation - rolling window {r_window_size}")
    plt.savefig("correlation.jpg")
 
############Plotting##################################
    df = full_df 

    plt.close()
    df['energy_mj'][0:].plot()
    plt.savefig("plot_energy.jpg")
 

    plt.close()
    plt.plot( df['power'][10:], label='Power', color='r' )
    plt.plot( df['funcs_tally'][10:], label='Functions Tally*'+str(func_fact), color='0.4' )
    plt.legend()
    plt.savefig("plot_power_full.jpg")
   
    plt.close()
    plt.plot( df['power'][400:1000], label='Power', color='r' )
    plt.plot( df['funcs_tally'][400:1000], label='Functions Tally*'+str(func_fact), color='0.4' )
    plt.legend()
    plt.savefig("plot_power_mid.jpg")

    plt.close()
    plt.plot( df['power'][400:700], label='Power', color='r' )
    plt.plot( df['funcs_tally'][400:700], label='Functions Tally*'+str(func_fact), color='0.4' )
    plt.legend()
    plt.savefig("plot_power_zoomed.jpg")

