import pandas as pd

column_names = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13']

df = pd.read_csv("access_log_Aug95.txt", delimiter=' ', names=column_names, encoding='unicode_escape')

errors1 = df[df['9'].notnull()]

df = df[df['9'].isnull()]


# Wrangling df
df[['14', '15']] = df['6'].str.split(' ', 1, expand=True)
df['15'] = df['15'].str.replace(f'HTTP/1.0', '')
df.drop(columns=['2', '3', '5', '6', '9', '10', '11', '12', '13'], inplace=True)
df.rename(columns={'1': 'Host/IP', '4': 'Timestamp', '14': 'Request Type', '15': 'Path', '7': 'Response', '8': 'Bytes'}, inplace=True)
df = df[['Host/IP', 'Timestamp', 'Request Type', 'Path', 'Response', 'Bytes']]


# Wrangling errors1
errors2 = errors1[errors1['10'].notnull()]

errors1 = errors1[errors1['10'].isnull()]

errors1[['14', '15']] = errors1['6'].str.split(' ', 1, expand=True)
errors1['15'] = errors1['15'].str.replace(f'HTTP/1.0', '')
errors1.drop(columns=['2', '3', '5', '6', '7', '10', '11', '12', '13'], inplace=True)
errors1.rename(columns={'1': 'Host/IP', '4': 'Timestamp', '14': 'Request Type', '15': 'Path', '8': 'Response', '9': 'Bytes'}, inplace=True)
errors1 = errors1[['Host/IP', 'Timestamp', 'Request Type', 'Path', 'Response', 'Bytes']]


#Wrangling errors2
errors2['6'] = errors2['6'] + errors2['7'] + errors2['8']
errors2[['14', '15']] = errors2['6'].str.split(' ', 1, expand=True)
errors2['15'] = errors2['15'].str.replace(f'HTTP/1.0', '')
errors2.drop(columns=['2', '3', '5', '6', '7', '8', '9', '12', '13'], inplace=True)
errors2.rename(columns={'1': 'Host/IP', '4': 'Timestamp', '14': 'Request Type', '15': 'Path', '10': 'Response', '11': 'Bytes'}, inplace=True)
errors2 = errors2[['Host/IP', 'Timestamp', 'Request Type', 'Path', 'Response', 'Bytes']]

wrangledDF = pd.concat([df, errors1, errors2])

wrangledDF['Timestamp'] = wrangledDF['Timestamp'].str.replace('[', '')

wrangledDF['Domain'] = [x.rsplit(".", 1)[-1] for x in wrangledDF['Host/IP']]

wrangledDF.to_csv('wrangledDF.csv', index=False)




