

import sys
import os,errno
import os.path
from bs4 import BeautifulSoup
import requests
import pandas as pd
import time
from multiprocessing import Pool,cpu_count,current_process
import re
import numpy as np
import collections
from fake_useragent import UserAgent
from datetime import datetime
from textblob import TextBlob
from proxycrawl.proxycrawl_api import ProxyCrawlAPI

inputPath = "productList.csv"

api = ProxyCrawlAPI({ 'token': 'Your API Key' })

directory = str(datetime.today().strftime("%Y%m%d"))

try:
    os.makedirs(directory)
except OSError as e:
    if e.errno != errno.EEXIST:
        raise

## -------------------------------------------------------------------------------------------
## Columns To Scrape from Amazon
## -------------------------------------------------------------------------------------------


columnList = ["productID","Country","Platform","Brand","Title",
              "Category",'AdjacentCategory', 'Segment',"Product Image",'Domestic Shipping','International Shipping',
              'Item model number', 'Product Dimensions','Shipping Weight', 'UPC',
              'Key Info','About','Product Desc']



domainList = {

        'CAN' : 'ca'
       
        }


languageList =   {
        'CAN' : 'ca'
        }  




## -------------------------------------------------------------------------------------------
## Functions
## -------------------------------------------------------------------------------------------

def Remove(duplicate):
    final_list = []
    for num in duplicate:
        if num not in final_list:
            final_list.append(num)
    return final_list

def dict2csv(x):
    return pd.DataFrame(dict([(k,pd.Series(v)) for k,v in x.items()]))

def returnProdcutNotAvailable(tags):
    
    tags['Brand'] = 'Product Not Available'
    tempDataFrame = dict2csv(tags)
            
    for x in columnList:
        if x not in tempDataFrame:
            tempDataFrame[x] = np.nan
    
    tempDataFrame = tempDataFrame[columnList]
    
    tempDataFrame = tempDataFrame.replace({'"':''}, regex=True)
    tempDataFrame.dropna(axis=0, how='all')
    return(tempDataFrame)

def extractFromAmazon(url,country,category,segment, ac):
          
    #ua = UserAgent()
    response = api.get(url)
    soup = BeautifulSoup(response['body'],"lxml")
    tags = collections.OrderedDict()
    
    tags['productID'] = url.split('/')[4]
    tags['Segment'] = segment
    tags['Category'] = category   
    tags['AdjacentCategory'] = ac     
    tags['Country'] = country
    
    orig_productID = tags['productID']    
    value = ""

    isEnglish = languageList[country] == 'en'
    
    
    
    # Platform extract
    tags['Platform'] = url.split('/')[2].split('.')[1]

        
    try:
        tags['Product Image'] = soup.find('img', {'id':'landingImage'})['data-a-dynamic-image'].split('":')[0].split('"',1)[1]
    except:
        if(soup.find('div', {'id':'g'})):
            return(returnProdcutNotAvailable(tags))
        elif(soup.find('div', {'id':'apsRedirectLink'})):
            if("We didn't find results" in soup.find('div', {'id':'apsRedirectLink'}).text):
                return(returnProdcutNotAvailable(tags))
        elif(soup.select('table td')):
            try:
                val = soup.select('table td')[1].b.text
            except:
                val = 'Nah'

            translatedVal = ""
            if isEnglish is False:
                try:
                    translatedVal = str(TextBlob(val).translate(from_lang = languageList[country], to='en'))   
                except:
                    if str(sys.exc_info()[0]) =="<class 'textblob.exceptions.NotTranslated'>":
                        translatedVal = val.strip()
                        pass
                    
            else:
                
                translatedVal = val.strip()
            
            translatedVal = translatedVal.replace(" ", "")

            possible_error_messages = ['Areyoulookingforsomething?', 'Lookingforsomething?', 'Lookingforapage?', 
            'Areyoulookingforsomethinginparticular?']
            if(translatedVal in (possible_error_messages)):
                return(returnProdcutNotAvailable(tags))
        else:
            print("Error: Product Image")
            raise AttributeError
        
    # Extract Brand
    try:
        if(soup.find('div', {'id':'brandBylineWrapper'})):
            try:
                tags['Brand'] = soup.find('div', {'id':'brandBylineWrapper'}).text.split('by')[1].strip()
            except:
                tags['Brand'] = soup.find('div', {'id':'brandBylineWrapper'}).text.strip()
        elif(soup.find('a', {'id':'bylineInfo'})):
            tags['Brand'] =  soup.find('a', {'id':'bylineInfo'}).text.strip()
        else:
            tags['Brand'] =  soup.find('a', {'id':'brand'}).text.strip()
    except:
        tags['Brand'] = 'BRAND_UNMAPPED'
    
    # Extract Title
    title = soup.find('span', {'id':'productTitle'}).text.strip()
    if isEnglish is False:
        try:
            tags['Title'] =   str(TextBlob(title).translate(from_lang = languageList[country], to='en'))  
        except:
            if str(sys.exc_info()[0]) =="<class 'textblob.exceptions.NotTranslated'>":
                tags['Title'] =   str(title).strip()
                pass        
    else:
        # tags['Title'] =   translator.translate(title)
        tags['Title'] =   str(title).strip()
    
    
    # Extract About Product
    temp = []  
    if(soup.select('div#featurebullets_feature_div div#feature-bullets li')):
        for features in soup.select('div#featurebullets_feature_div div#feature-bullets li'):
                temp.append(features.text.strip())
    elif(soup.select('div#feature-bullets-btf div.content li')):
        for features in soup.select('div#feature-bullets-btf div.content li'):
                temp.append(features.text.strip())
    else:
        temp = []
    
    about =  ' | '.join(temp)
    
    emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags=re.UNICODE)
    about = emoji_pattern.sub(r'', about) # no emoji

    if isEnglish is False: 
        try:
            tags['About'] =  str(TextBlob(about).translate(from_lang = languageList[country], to='en'))  
        except:
            if str(sys.exc_info()[0]) =="<class 'textblob.exceptions.NotTranslated'>":
                tags['About'] = str(about).strip()
                pass           

    else:
        # tags['About'] = translator.translate(about)
        tags['About'] = str(about).strip()
    
    
    # Extract Key Information
    if(soup.find('div', {'class':'disclaim'})):
        tags['Key Info'] = soup.find('div', {'class':'disclaim'}).text.strip()
      
    # Extrcat Product Description
    if(soup.find('div', {'id':'productDescription'})):
        prodDesc = re.sub("\n|\xa0", " ", soup.find('div', {'id':'productDescription'}).p.text.strip())

        if isEnglish is False:
            try:
                tags['Product Desc'] = str(TextBlob(prodDesc).translate(from_lang = languageList[country], to='en'))  
            except:
                if str(sys.exc_info()[0]) =="<class 'textblob.exceptions.NotTranslated'>":
                    tags['Product Desc'] = str(prodDesc).strip()
                    pass   
        else:
            
            tags['Product Desc'] = str(prodDesc).strip()
        

    if soup.select('table#productDetails_detailBullets_sections1 tr'):
        # print('Data available as Table')
        for li in soup.select('table#productDetails_detailBullets_sections1 tr'):
          try:
            key = li.th.text.strip()
            if(key == 'ASIN'):
                key = 'productID'
            if(key in ('Package Dimensions','Größe und/oder Gewicht','Produktabmessungen')):
                key = 'Product Dimensions'
            if(key in ('Versand:')):
                key = 'International Shipping'
            if(key in ('Modellnummer')):
                key = 'Item model number'
            if(key == 'Best Sellers Rank'):
                continue
                temp = []
                for sp in li.td.select('span span'):
                    temp.append(sp.text.strip())
                value = temp
                tags['Amazon Best Sellers Rank'] = value
            elif(key in ('Versand:')):
                key = 'International Shipping'
                value = li.td.text.strip()
                
                if isEnglish is False:
                    try:
                        tags[key] = str(TextBlob(value).translate(from_lang = languageList[country], to='en'))  
                    except:
                        if str(sys.exc_info()[0]) =="<class 'textblob.exceptions.NotTranslated'>":
                            tags[key] = str(value).strip()
                            pass                   
                else:
                    # tags[key] = translator.translate(value)
                    tags[key] = str(value).strip()
            else:
                value = li.td.text.strip()
                tags[key] = value
          except AttributeError:
            continue
    elif soup.select('div#prodDetails table tr'):
        # print('Data available as 2 tables)
        for tr in soup.select('div#prodDetails table tr'):
              try:
                  key = tr.select_one('td.label').text.strip()
                  if(key == 'ASIN'):
                    key = 'productID'
                  if(key in ('Package Dimensions','Größe und/oder Gewicht','Produktabmessungen')):
                    key = 'Product Dimensions'
                  if(key in ('Versand:')):
                    key = 'International Shipping'
                  if(key in ('Modellnummer')):
                      key = 'Item model number'
                  value = tr.select_one('td.value').text.strip()
              except AttributeError:
                  continue
              if(key == 'International Shipping'):
                  try:
                      tags[key] = str(TextBlob(value).translate(from_lang = languageList[country], to='en'))  
                  except:
                      if str(sys.exc_info()[0]) =="<class 'textblob.exceptions.NotTranslated'>":
                          tags[key] = str(value).strip()
                          pass                    
              else:
                  tags[key] = str(value).strip()
    elif soup.select('div.content ul li'):
        # print('Data available as Section')
        for li in soup.select('div.content ul li'):
            try:                
                title = li.b
                key = title.text.strip().rstrip(':')
                key = re.sub("\n|\xa0", " ", key)
                if(key == 'ASIN'):
                    key = 'productID'
                if(key in ('Package Dimensions','Größe und/oder Gewicht','Produktabmessungen')):
                    key = 'Product Dimensions'
                if(key in ('Modellnummer')):
                      key = 'Item model number'
                
                elif(key == 'Average Customer Review'):
                    value = li.select('span.a-icon-alt').text.strip()
                elif(key in ('Versand:')):
                    key = 'International Shipping'
                    tval = re.sub("\(", "", title.next_sibling.strip())
                    if isEnglish is False:
                        try:
                            value = str(TextBlob(tval).translate(from_lang = languageList[country], to='en'))  
                        except:
                            if str(sys.exc_info()[0]) =="<class 'textblob.exceptions.NotTranslated'>":
                                value = str(tval).strip()
                                pass                      
                    else:
                        # value = translator.translate(tval)
                        value = str(tval).strip()
                    
                else:
                    value = re.sub("\(", "", title.next_sibling.strip())
                tags[key] = value
            except AttributeError:
                # print('Attribute Error')
                continue

    tags['productID'] = orig_productID
    tempDataFrame = dict2csv(tags)
    
    for x in columnList:
        if x not in tempDataFrame:
            tempDataFrame[x] = np.nan
    
    tempDataFrame = tempDataFrame[columnList]
    
    tempDataFrame = tempDataFrame.replace({'"':''}, regex=True)
    tempDataFrame.dropna(axis=0, how='all')
    
    del temp, tags
    return(tempDataFrame)
    
def scrapeAmazon(AsinList):
    
    outputPath = directory + '/extractedData_' + current_process().name + '.csv' 
    outputPath2 = directory + '/extractedData_' + current_process().name + 'unknown_asin.csv' 
    #configFile = 'masterConfig_' + current_process().name + '.txt'

    # AsinList = AsinList.iloc[:,0]
    tempDataFrame = pd.DataFrame(data=None)
    tempDataFrame2 = pd.DataFrame(data=None)
    

    try:
        with open(outputPath, 'r') as detailsFile:
            print('Master Data file exists')
    # generate the file if doesn't exist
    except IOError:
        with open(outputPath, 'w') as detailsFile:        
            tempDataFrame = pd.DataFrame(columns=columnList)
            tempDataFrame.to_csv(outputPath,index=False,encoding = 'utf-8')
            #print('Creating header file')


    # check if not found file exists
    try:
        with open(outputPath2, 'r') as detailsFile2:
            print('Unknown ASIN file exists')
    # generate the file if doesn't exist
    except IOError:
        with open(outputPath2, 'w') as detailsFile2:        
            tempDataFrame2 = pd.DataFrame(columns=columnList)
            tempDataFrame2.to_csv(outputPath,index=False,encoding = 'utf-8')
            #print('Creating unknown asin header file')




        detailsFile = open(outputPath, 'w',encoding = 'utf-8')
        tempDataFrame = pd.DataFrame(columns=columnList)
        tempDataFrame.to_csv(detailsFile,index=False,encoding = 'utf-8')
            # print('Rewriting Unknown ASIN file')
        detailsFile2 = open(outputPath2, 'w',encoding = 'utf-8')
        tempDataFrame2 = pd.DataFrame(columns=columnList)
        tempDataFrame2.to_csv(detailsFile2,index=False,encoding = 'utf-8')
            
           

    nRows = len(AsinList.index)
    currIndex = 1
    
    for index,row in AsinList.iterrows():
        #clearFile = 0
        url = "https://www.amazon." + domainList[row['Country']] +"/dp/"+ row['productID']
        
        print ("Worker (" + str(current_process().name.replace('SpawnPoolWorker-','').replace(')','')) + ") Status -> " + str(currIndex) + "/" + str(nRows) )
        currIndex += 1
        
        try:
            newDataFrame = extractFromAmazon(url,row['Country'],row['Category'],row['Segment'], row['AdjacentCategory'] )
        except AttributeError as e:
            #print("This is the error coming from function (extractFromAmazon): " + str(e))
            tags = collections.OrderedDict()
            tags['productID'] = row.loc['productID']
            tags['Segment'] = row.loc['Segment']
            tags['Category'] = row.loc['Category'] 
            tags['AdjacentCategory'] = row.loc['AdjacentCategory'] 
            tags['Country'] = row.loc['Country']
            newDataFrame = returnProdcutNotAvailable(tags)
            newDataFrame['Brand'] = 'Attribute Error'
        newDataFrame = newDataFrame.replace({'"':''}, regex=True)
        if newDataFrame.loc[0, 'Brand'] in ['Product Not Available','Attribute Error']:
            newDataFrame.to_csv(detailsFile2, header=False,index = False)    
            # print("Done Writing to csv ", len(newDataFrame))
            tempDataFrame2 = pd.concat([tempDataFrame2,newDataFrame],axis = 0)
            tempDataFrame2.dropna(axis=0, how='all')
        else:
            newDataFrame.to_csv(detailsFile, header=False,index = False)
            # print("Done Writing to csv", len(newDataFrame))
            tempDataFrame = pd.concat([tempDataFrame,newDataFrame],axis = 0)
            tempDataFrame.dropna(axis=0, how='all')
        
       
        time.sleep(10)
            
    del newDataFrame, tempDataFrame2, detailsFile, detailsFile2
    return(tempDataFrame)
    
def mergeDir(directory):
    df_list = pd.DataFrame()
    temp_df = pd.DataFrame()
    for file in os.listdir(directory):
        try:
            df = pd.read_csv(directory + '/' + file)
        except:
            os.remove(directory+'/'+file)
            continue

        df_list = df_list.append(df)
        os.remove(directory+'/'+file)
    if len(df_list) > 0:
        df_list.drop_duplicates(inplace=True)
        temp_df = df_list.copy()
        df_list_unknown_asin = df_list[df_list['Brand'].isin(['Product Not Available','Attribute Error'])].copy()
        df_list = df_list[~df_list['Brand'].isin(['Product Not Available','Attribute Error'])].copy()
        df_list.to_csv(directory + '/ExtractedData.csv', index=False)
        df_list_unknown_asin.to_csv(directory + '/ExtractedData_unknown_list.csv', index=False)
        del df_list, df_list_unknown_asin
    return temp_df

Error_name = 'IndexError'
attempt = 1

## -------------------------------------------------------------------------------------------
## Main Function
## -------------------------------------------------------------------------------------------    

if __name__ == "__main__":    
    while Error_name == 'IndexError':
        try:
            AsinList = pd.read_csv(inputPath)
            #AsinList = AsinList.sample(30)
            AsinList.loc[AsinList.Category.isnull(), 'Category'] = AsinList['AdjacentCategory']
            
            len1 = len(AsinList)
            AsinList['productID'] = AsinList['productID'].astype(str)
            tdf = AsinList.copy()
            isPresent = False
        
            df_merge_current = mergeDir(directory)
            if len(df_merge_current) > 0:
                print("Previous files found, merging..")
                df_merge_current = df_merge_current[AsinList.columns]
                df_merge_current['productID'] = df_merge_current['productID'].astype(str)
                AsinList = pd.concat([AsinList, df_merge_current, df_merge_current]).drop_duplicates(keep=False)
                print("Done merging, scraping for remaining data")
        
            else:
                print("No previous files found")
        
            if len(AsinList) == 0:
                print("Already done scraping, no new data to scrape")
                quit()
            
            NUM_CORES = 1
            
            print('using',NUM_CORES,'cores')
            
            df_split = np.array_split(AsinList,NUM_CORES)
            p = Pool(NUM_CORES)
            
            
            result_list = p.imap(scrapeAmazon, df_split)
            dfout = pd.concat(result_list)
            p.close()
            p.join()
            
            # Remove duplicates after successful completion
            
            mergeDir(directory)
            Error_name = 'NoError'
            print ('---The file has been created---')
			
        except IndexError:
            p.close()
            p.join()            
            if attempt%10 == 0:
                time.sleep(60)
            else:
                time.sleep(20)
            attempt = attempt+1
            print('Attempt Number : ', attempt)
            pass
        except KeyboardInterrupt:          
            Error_name = 'KeyboardInterrupt'
            