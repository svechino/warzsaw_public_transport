#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import re
import numpy as np


# In[2]:


stops = pd.read_csv("stops.txt")
stops.head()


# In[3]:


stops.info()


# In[4]:


stops = stops.drop(columns = ['location_type', 'stop_IBNR', 'stop_PKPPLK', 'platform_code', 'wheelchair_boarding'])


# In[5]:


stops.describe()


# In[6]:


stops['stop_id'].nunique()


# In[7]:


stops_unique_ids = stops['stop_id'].unique()


# In[8]:


stops_time = pd.read_csv("stop_times.txt")


# In[9]:


stops_time.head()


# In[10]:


stops_time.info()


# In[11]:


stops_time['stop_id'].nunique()


# In[12]:


stops_time['stop_id'].unique()


# In[13]:


stops_time_ids = stops_time['stop_id'].unique()


# In[14]:


# Function for cleaning stop ids:
def clean_stop_id(stop):
    match = re.search(r'\d+', str(stop))  
    return match.group(0) if match else None  

# stops.txt
stops['clean_stop_id'] = stops['stop_id'].apply(clean_stop_id)
stops_cleaned = set(stops['clean_stop_id'].dropna())


# In[15]:


stops['stop_id'].nunique()


# In[16]:


stops_time['stop_id'] = stops_time['stop_id'].astype(str)
stop_times_set = set(stops_time['stop_id'])


# In[17]:


# Removing all letters and symbols:
def remove_letters(stop_id):
    return re.sub(r'\D', '', stop_id)  


stops["stop_id"] = stops["stop_id"].astype(str).apply(remove_letters)
stops_time["stop_id"] = stops_time["stop_id"].astype(str).apply(remove_letters)


# In[18]:


stops['stop_id_numeric'] = stops['stop_id'].str.extract(r'(\d+)')  
stops['stop_id_numeric'] = stops['stop_id_numeric'].astype(str)  


# In[19]:


# Сalculating coincidences:
matching_ids = set(stops["stop_id"]) & set(stops_time["stop_id"])
missing_in_stops = set(stops_time["stop_id"]) - set(stops["stop_id"])
missing_in_stop_times = set(stops["stop_id"]) - set(stops_time["stop_id"])


# In[20]:


print(f" Number of concidenced stop_id: {len(matching_ids)}")
print(f" Number stop_id, that are not in stops.txt: {len(missing_in_stops)}")
print(f" Number stop_id, that are not in stop_times.txt: {len(missing_in_stop_times)}")


# Checking hypotesis that these stops belongs to metro

# In[21]:


stops_time["hour"] = stops_time["arrival_time"].str[:2].astype(int)
stops_time.head()


# In[22]:


trips = pd.read_csv("trips.txt")
trips.head()


# In[23]:


trips.info()


# In[24]:


stops_time['trip_id'] = stops_time['trip_id'].str.strip().str.replace(r'_$', '', regex=True)
trips['trip_id'] = trips['trip_id'].str.strip().str.replace(r'_$', '', regex=True)


# In[25]:


stop_trips_with_routes = stops_time.merge(trips, how='left', on='trip_id')


# In[26]:


stop_trips_with_routes.head()


# In[27]:


routes = pd.read_csv('routes.txt')
routes.head()


# In[28]:


routes['route_type'].unique()


# In[29]:


routes = routes.drop(columns=['agency_id', 'route_color', 'route_text_color', 'route_sort_order'])


# In[30]:


stops_with_types = stop_trips_with_routes.merge(routes, how='left', on='route_id')


# In[31]:


metro = stops_with_types[stops_with_types['route_type']==1]
metro['stop_id'].nunique()


# In[32]:


stops_with_types = stops_with_types.drop(columns=['shape_dist_traveled', 'wheelchair_accessible', 'bikes_allowed', 'route_short_name'])


# In[33]:


stops_total_info = stops_with_types.merge(stops, how='left', on='stop_id')


# In[34]:


stops_total_info = stops_with_types.merge(stops, left_on='stop_id', right_on='stop_id_numeric', how='left')


# In[35]:


stops_total_info.count()


# In[36]:


frequencies = pd.read_csv("frequencies.txt", dtype={'trip_id': str})
frequencies.head()


# In[37]:


frequencies = frequencies.merge(trips[['trip_id', 'route_id']], on='trip_id', how='left')


# In[38]:


frequencies = frequencies.merge(routes[['route_id', 'route_type']], on='route_id', how='left')


# In[39]:


frequencies.head()


# In[40]:


frequencies.info()


# In[41]:


print(frequencies.columns)
print(routes.columns)


# In[42]:


# Function for generation metro trips
def generate_metro_trips(row):
    times = pd.date_range(start=row['start_time'], end=row['end_time'], freq=f"{row['headway_secs']}S").time
    return pd.DataFrame({'trip_id': row['trip_id'], 'departure_time': times, 'route_id': row['route_id'], 'route_type': row['route_type']})


# In[43]:


def fix_large_hours(time_str):
    """
    Fixing time errors (for example, 25:30:00 → 01:30:00).
    """
    if isinstance(time_str, str):
        match = re.match(r'(\d+):(\d{2}):(\d{2})', time_str)
        if match:
            hour, minute, second = map(int, match.groups())
            if hour >= 24:
                hour -= 24  
                return f"{hour:02}:{minute:02}:{second:02}"
    return time_str  

frequencies["start_time"] = frequencies["start_time"].apply(fix_large_hours)
frequencies["end_time"] = frequencies["end_time"].apply(fix_large_hours)

print("Times errors are fixed")


# In[44]:


metro_trips = pd.concat(frequencies.apply(generate_metro_trips, axis=1).tolist(), ignore_index=True)
print("Metro trips are generated")
print(metro_trips.head())


# In[45]:


metro_trips.info()


# In[46]:


metro_trips["departure_time"] = metro_trips["departure_time"].astype(str)
metro_trips["hour"] = metro_trips["departure_time"].str[:2].astype(int) 


# In[47]:


metro_trips["hour"] = metro_trips["departure_time"].str[:2].astype(int) 


# In[48]:


metro_trips.head()


# In[49]:


metro_hourly = metro_trips.groupby(["route_id", "hour"]).size().reset_index(name="number_of_trips")


# In[50]:


metro_hourly.head()


# In[51]:


metro_data = stops_total_info[stops_total_info['route_type']==1].copy()


# In[52]:


metro_data.columns


# In[53]:


metro_data


# In[54]:


metro_data = metro_data.drop(columns=['hour'])


# In[55]:


metro_data["hour"] = stops_total_info["departure_time"].str.split(":").str[1].astype(int)
metro_data


# In[56]:


metro_data = metro_data.drop(columns=['trip_id', 'arrival_time', 'departure_time', 'stop_id_x',
                                      'pickup_type', 'drop_off_type', 'stop_id_y', 'clean_stop_id'])


# In[57]:


metro_data.info()


# In[58]:


metro_data = metro_data.merge(metro_hourly, on=['route_id', 'hour'], how='left')


# In[59]:


metro_data


# In[60]:


metro_data['trip_id'] = metro_data['route_id']


# In[61]:


metro_hourly["hour"] = metro_hourly["hour"].astype(int)


# In[63]:


stops_total_info["hour"].unique()


# In[64]:


# Adjusting hours to 0-23 format: 
stops_total_info["hour"] = stops_total_info["hour"].apply(lambda x: x - 24 if x >= 24 else x)
print(stops_total_info["hour"].unique())


# In[65]:


stops_total_info["hour"] = stops_total_info["hour"].astype(int)


# In[66]:


print(stops_total_info.columns)
print(metro_data.columns)


# In[67]:


print(stops_total_info.info())
print(metro_data.info())


# In[68]:


stops_total_info = stops_total_info.drop(columns=[
    'arrival_time', 'departure_time', 'stop_id_x', 'pickup_type', 
    'drop_off_type', 'stop_id_y','clean_stop_id' 
])


# In[69]:


missing_cols = set(stops_total_info.columns) - set(metro_data.columns)
for col in missing_cols:
    metro_data[col] = None  


# In[70]:


print(stops_total_info.dtypes)
print(metro_data.dtypes)


# In[72]:


duplicates = stops_total_info.duplicated(subset=['trip_id', 'stop_id_numeric', 'hour'], keep=False)
print(f"Trips Duplicates: {duplicates.sum()}")


# In[73]:


stops_total_info = stops_total_info.drop_duplicates()


# In[74]:


stops_total_info['number_of_trips']=1


# In[77]:


stops_total_info['number_of_trips']= stops_total_info['number_of_trips'].astype(int)


# In[78]:


final_data = pd.concat([stops_total_info, metro_data], ignore_index=True)


# In[83]:


metro_data['hour'].unique()


# In[79]:


final_data


# In[81]:


final_data['hour'].unique()


# In[80]:


final_data.to_csv('final_data.csv', sep=',', encoding='utf-8-sig', index=False)

