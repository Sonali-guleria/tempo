def custom_resample_func(data):
  """
  This is the custom function you want to execute on the minute bars. Override this function
  """
  # data - has only 1 minute worth of data
  data_agg = data.copy()
  #.iloc[0:1] # SG-remove these indexes, very messy 
  data_agg["diff_mean"] = (data_agg["Price"]-data["Price"].mean()).round(3)
  
  return(data_agg)