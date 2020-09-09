import requests
import json
import shutil
import datetime as dt
import sys
import pandas as pd
import time

proxyServers = {'https':'XX.XX.XX.XX:XXXX'}
userName = ''
passWord = ''

#Step 1: token request
#If you already have authentication token you can copy and paste it here
#The token will be validated and used if found to be valid
#Otherwise a new token will be requested and used

token = '_hXrFKdNaMDj-__pzI9Nhtqa45oAWZzLHKyN5-bSRXa5b3v0Qayrq7rgSwJetK0_JZ40nEr3Il3V9HTg-rgvdn-zh-E53o0J6JSJ7V6haTil75k7AhGdQj0GgKgvgwoCYwgJKSn7Wr4KIiLgDYO-OBegbpeFrNQOReeEC_efnmGQqmMci89RI-WAsLvNLWltO3hgIODep2H9UgTaw0DX2LQxTTjCW8ruNATTjgeQovthU1X2qexmf6rWEM78Mp8HUjPyrX1gvLsNP7TDypPq6I9EQn4PDv6UeLCEwmoedq-M'

def NewToken(un, pw):
    if not (un and pw):
        print ('Username or password is empty. Please type in your username and password in the Step1 section of the code and try again.')
        sys.exit()
    
    requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Authentication/RequestToken'    
    requestHeaders = {
        'Prefer':'respond-async',
        'Content-Type':'application/json'
        }
    requestBody = {
        'Credentials': {
        'Username': un,
        'Password': pw
      }
    }
    r1 = requests.post(requestUrl, json = requestBody, headers = requestHeaders, proxies = proxyServers)
    
    if r1.status_code == 200 :
        jsonResponse = json.loads(r1.text.encode('ascii', 'ignore'))
        return jsonResponse["value"]
    else:
        print ('Please type in valid username and password in the Step1 section of the code and try again.')
        sys.exit()
    
if token:
    #check the validity of the authentication token
    requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Users/Users(' + userName + ')'
    requestHeaders = {
        'Prefer':'respond-async',
        'Content-Type':'application/json',
        'Authorization': 'token ' + token
    }
    r1 = requests.get(requestUrl, headers = requestHeaders, proxies = proxyServers)
    #if the response status is 200 OK then the token is valid, otherwise request new token
    if r1.status_code != 200:
        token = NewToken(userName, passWord)
else:
    #token is empty, request new token.
    token = NewToken(userName, passWord)

print('using authorization token:')
print(token)

#Step 2: create instrument list using search

requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Search/FuturesAndOptionsSearch'

requestHeaders = {
    'Prefer':'respond-async;odata.maxpagesize=5000',
    'Content-Type':'application/json',
    'Authorization': 'token ' + token
}

requestBody = {
    'SearchRequest': {
      '@odata.context': 'http://hosted.datascopeapi.reuters.com/RestApi/v1/$metadata#ThomsonReuters.Dss.Api.Search.FuturesAndOptionsSearchRequest',
      'FuturesAndOptionsType': 'Options',
      'UnderlyingRic': '.SPX',
      'ExpirationDate': {
        '@odata.type': '#ThomsonReuters.Dss.Api.Search.DateValueComparison',
        'ComparisonOperator': 'GreaterThanEquals',
        'Value': str(dt.date.today())
      }
    }
}

print('requesting list of option RICs from ' + requestUrl)
r2 = requests.post(requestUrl, json = requestBody, headers = requestHeaders, proxies = proxyServers)
r2Json = json.loads(r2.text.encode('ascii', 'ignore'))

#Search uses server driven paging, which limits the result set to the max of 250 rows,
#unless odata.maxpagesize preference is set in the request header.
#Subsequent pages can be retrieved using the nextlink
#and continuing to call the next link in each received payload
#until there is no next link in the payload indicating that no more data is available.
instrumentList = r2Json['value']
nextLink = r2Json['@odata.nextlink'] if '@odata.nextlink' in r2Json else False

while nextLink:
    print('requesting the next batch of option RICs from ' + nextLink)
    r2 = requests.post(nextLink, json = requestBody, headers=requestHeaders, proxies = proxyServers)
    r2Json = json.loads(r2.text.encode('ascii', 'ignore'))
    instrumentList = instrumentList + r2Json['value']
    nextLink = r2Json['@odata.nextlink'] if '@odata.nextlink' in r2Json else False

print(str(len(instrumentList)) + ' option RICs returned from search')
#Transform instrument list into the format required for extraction
tmpDF = pd.DataFrame.from_dict(instrumentList, orient='columns')
tmpDF = tmpDF[['Identifier','IdentifierType']]
instrumentList = tmpDF.to_dict('records')

###Step 3: send an on demand extraction request using the received token 

requestUrl='https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractRaw'

requestHeaders={
    'Prefer':'respond-async',
    'Content-Type':'application/json',
    'Authorization': 'token ' + token
}

requestBody={
  'ExtractionRequest': {
    '@odata.type': '#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.ElektronTimeseriesExtractionRequest',
    'ContentFieldNames': [
      'RIC',
      'Expiration Date',
      'Put Call Flag',
      'Trade Date',
      'Bid',
      'Ask',
      'Last',
      'Security Description'
    ],
    'IdentifierList': {
      '@odata.type': '#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList',  
      'InstrumentIdentifiers': instrumentList,
    },    
    'Condition': {
      'StartDate': str(dt.date.today() - dt.timedelta(days=5)),
      'EndDate': str(dt.date.today())
    }
  }
}

print('sending the extraction request for the list of option RICs')
r3 = requests.post(requestUrl, json=requestBody, headers=requestHeaders, proxies = proxyServers)

#Display the response status, and the location url to use to get the status of the extraction request
#Initial response status after approximately 30 seconds wait will be 202
print ('response status from the extraction request = ' + str(r3.status_code))

#If there is a client side or server side error, display the error information and exit
if r3.status_code >= 400 :
    print(r3.text.encode('ascii', 'ignore'))
    sys.exit()

#Step 4: poll the status of the request using received location URL, and get the jobId and extraction notes

requestUrl = r3.headers['location']

requestHeaders={
    'Prefer': 'respond-async',
    'Content-Type': 'application/json',
    'Authorization': 'token ' + token
}

r4 = requests.get(requestUrl, headers=requestHeaders, proxies = proxyServers)

#The extraction may take a long time for large content sets
#While the extraction is being processed on the server the request status we receive is 202
#We're polling the service for the status every 30 seconds until the status is 200

while r4.status_code == 202 :
    r4 = requests.get(requestUrl, headers=requestHeaders, proxies = proxyServers)
    print (str(dt.datetime.now()) + ' Server is still processing the extraction. Checking the status again in 30 seconds')
    time.sleep(30)


###Step 5: get the extraction results using received jobId and save compressed data to disk

fileName = 'C:/Temp/SPXOptions.csv.gz'

#When the status of the request is 200 the extraction is complete, we display the jobId and the extraction notes
if r4.status_code == 200 :
    r4Json = json.loads(r4.text.encode('ascii', 'ignore'))
    jobId = r4Json["JobId"]
    print ('jobId: ' + jobId + '\n')
    notes = r4Json["Notes"]
    print ('Extraction notes:\n' + notes[0])
    requestUrl = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/RawExtractionResults' + "('" + jobId + "')" + '/$value'
    requestHeaders={
        'Prefer': 'respond-async',
        'Content-Type': 'text/plain',
        'Accept-Encoding': 'gzip',
        'Authorization': 'token ' + token
    }
    r5 = requests.get(requestUrl,headers=requestHeaders,stream=True)
    #Ensure we do not automatically decompress the data on the fly:
    r5.raw.decode_content = False
    print ('Response headers for content: type: ' + r5.headers['Content-Type'] + ' - encoding: ' + r5.headers['Content-Encoding'] + '\n')
    fo = open(fileName, 'wb')
    shutil.copyfileobj(r5.raw, fo)
    fo.close()
    print ('Saved compressed data to file:' + fileName)





