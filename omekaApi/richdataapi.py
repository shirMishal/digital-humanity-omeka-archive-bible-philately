import os
import io
import sys
import time
import requests
import json
import csv
import pandas

#dict {"numAlbum/numPage": omeka_id}
id_by_numberField = {}
#dict {"itemset": omeka_id}
id_by_itemSet = {}
#dict {"bible ref": omeka_id}
id_by_reference = {}
#dict {"stamp identifier": omeka_id}
id_by_stampID = {}

def extract_dict_id_by_itemSet(json_object):
    for dict in json_object:
        key = dict['o:title']
        val = dict['o:id']
        id_by_itemSet [key] = val
    return id_by_itemSet



def omekaApiItemSets():
    install_location = "http://site_name.reclaim_hoasting_domain"
    # generate key_identity and key_credential with your omeka acount
    # mor info: https://omeka.org/s/docs/user-manual/admin/users/#api-key
    key_identity = 'put your string here'
    key_credential = 'put your string here'
    endpoint = "{}/api/item_sets?key_identity={}&key_credential={}"
    final_uri = endpoint.format(install_location, key_identity, key_credential)
    headers = {
        'accept': 'application/json'
    }
    try:
        response = requests.get(final_uri, headers=headers)
    except requests.RequestException as e:
        dict(error=str(e))
    print (response)
    data = response.json()
    id_by_itemSet = extract_dict_id_by_itemSet(data)
    print (id_by_itemSet)

def extract_dict_id_stampIdentifier(json_object):
    for dict in json_object:
        key = dict.get('dcterms:identifier')
        if key!= None:
            key = key[0]['@value']
            val = dict['o:id']
            id_by_stampID [key] = val
    return id_by_stampID

def extract_dict_id_pageIdentifier(json_object):    
    for dict in json_object:
        key = dict['dcterms:identifier'][0]['@value']
        val = dict['o:id']
        id_by_numberField [key] = val
    return id_by_numberField

def extract_dict_id_refIdentifier(json_object):    
    for dict in json_object:
        key = dict['o:title']
        val = dict['o:id']
        id_by_reference [key] = val
    return id_by_reference
        

def omekaApiItems(item_set_id):
    install_location = "http://site_name.reclaim_hoasting_domain"
    # generate key_identity and key_credential with your omeka acount
    # mor info: https://omeka.org/s/docs/user-manual/admin/users/#api-key
    key_identity = 'put your string here'
    key_credential = 'put your string here'
    endpoint = "{}/api/items?key_identity={}&key_credential={}&item_set_id={}"
    final_uri = endpoint.format(install_location, key_identity, key_credential, item_set_id)
    headers = {
        'accept': 'application/json'
    }
    try:
        response = requests.get(final_uri, headers=headers)#data={'data': json.dumps(items)},
    except requests.RequestException as e:
        dict(error=str(e))
    print (response)
    return response.json()
    

 

def editRefs(csvFile):
    #open and read csv file  "user_id": int,
    dataTypes={"o:is_public":"string","o:owner":"string","dcterms:resource_class":"string","o:resource_template":"string","o:item_set":"string","dcterms:title":"string","dcterms:description":"string","bibo:volume":"string","bibo:chapter":"string","bibo:number":"string","dcterms:bibleVersion":"string","dcterms:language":"string","dcterms:isPartOf":"string"}
    df = pandas.read_csv(csvFile, dtype=dataTypes)
    print (df)
    print(type(df[['dcterms:isPartOf']].values[0]))#returns <class 'numpy.ndarray'> (obj: ['05/02'])
    print(df[['dcterms:isPartOf']].values[0].tolist()[0]) #identifier to a page
    omekaApiItems(13)
    data = omekaApiItems(id_by_itemSet.get("Pages"))
    dict_id_pageIdentifier = extract_dict_id_pageIdentifier(data)
    print (dict_id_pageIdentifier)

    r = len(df.index)
    for i in range(r):
        #df.at[row, col] = val
        print (df.at [i, 'dcterms:isPartOf'])
        if (id_by_numberField.get(df.at [i, 'dcterms:isPartOf']) != None):
            df.at [i, 'dcterms:isPartOf'] = str(id_by_numberField[df.at [i, 'dcterms:isPartOf']])
        else:
            df = df.drop(i)
            print ("delete")
    print (df)    
    df.to_csv(os.path.splitext(os.path.basename(csvFile))[0]+"_output.csv", index=False) #to write df into new file


def editQuotes(csvFile):
    #open and read csv file  "user_id": int,
    dataTypes={"o:is_public":"string","o:owner":"string","dcterms:resource_class":"string","o:resource_template":"string","o:item_set":"string","dcterms:title":"string","dcterms:description":"string","dcterms:bibleVersion": "string","dcterms:language": "string","dcterms:isPartOf":"string"}
    df = pandas.read_csv(csvFile, dtype=dataTypes, usecols=dataTypes.keys())
    print (df)
    print(type(df[['dcterms:isPartOf']].values[0]))#returns <class 'numpy.ndarray'> (obj: ['05/02'])
    print(df[['dcterms:isPartOf']].values[0].tolist()) #identifier to a page
    #build dict pages- id
    data = omekaApiItems(id_by_itemSet.get("Pages"))
    dict_id_pageIdentifier = extract_dict_id_pageIdentifier(data)
    print (dict_id_pageIdentifier)
    #build dict bible_ref- id
    data = omekaApiItems(id_by_itemSet.get("Bible references"))
    dict_id_by_reference = extract_dict_id_refIdentifier(data)
    print (dict_id_by_reference)

    r = len(df.index)
    for i in range(r):
        #df.at[row, col] = val
        print (df.at [i, 'dcterms:isPartOf'])
        pages = (df.at [i, 'dcterms:isPartOf']).split(";")
        pages_id = []
        for page in pages:
            if (id_by_numberField.get(page) != None): #page has omeka id
                pages_id.append(str(id_by_numberField.get(page)))
        print ((';').join(pages_id))
        if len(pages_id) != 0: 
            df.at [i, 'dcterms:isPartOf'] = (';').join(pages_id) 
        print (df.at [i, 'dcterms:description'])
        if (id_by_reference.get(df.at [i, 'dcterms:description']) != None):
            df.at [i, 'dcterms:description'] = str(id_by_reference[df.at [i, 'dcterms:description']])
            print(df.at [i, 'dcterms:description'])
        if len(pages_id) == 0:
            df = df.drop(i)
            print ("delete")  
    print (df)    
    df.to_csv(os.path.splitext(os.path.basename(csvFile))[0]+"_output.csv", index=False) #to write df into new file


def editStamps(csvFile):
    #open and read csv file  "user_id": int,
    dataTypes={"o:is_public":"string","o:owner":"string","dcterms:resource_class":"string","dcterms:isFormatOf":"string","dcterms:number" :"string" ,"o:resource_template":"string","o:item_set":"string","dcterms:title":"string","dcterms:description":"string","dcterms:issued": "string","dcterms:language": "string","dcterms:coverage" :"string","dcterms:type" : "string","dcterms:isPartOf":"string","dcterms:identifier" :"string","media:link":"string","dcterms:references":"string"}                                                                                                                                                                                                                                                                                                                
    cols_for_stamps = [e for e in dataTypes.keys() if e not in ('dcterms:isFormatOf','dcterms:number')]
    df_stamps = pandas.read_csv(csvFile, dtype=dataTypes, usecols=cols_for_stamps , encoding='utf-8')
    df_stamps = df_stamps[df_stamps ["o:resource_template"] == 'Stamp']
    # reset indexes of rows
    df_stamps = df_stamps.reset_index(drop=True)
    print (df_stamps)
    # print(type(df[['dcterms:isPartOf']].values[0]))#returns <class 'numpy.ndarray'> (obj: ['05/02'])
    # print(df[['dcterms:isPartOf']].values[0].tolist()) #identifier to a page
    #build dict pages- id
    data = omekaApiItems(id_by_itemSet.get("Pages"))
    dict_id_pageIdentifier = extract_dict_id_pageIdentifier(data)
    print (dict_id_pageIdentifier)
    
    r = len(df_stamps.index)
    # for i in range(r):
    #     print (df_stamps.at [i, 'dcterms:isPartOf'])
    for i in range(r):
        #df.at[row, col] = val
        print (df_stamps.at [i, 'dcterms:isPartOf'])
        #pages = (df_stamps.at [i, 'dcterms:isPartOf']).replace(" ","").split(";")
        pages = (df_stamps.at [i, 'dcterms:isPartOf']).split(";")
        pages_id = []
        for page in pages:
            if (id_by_numberField.get(page) != None): #page has omeka id
                pages_id.append(str(id_by_numberField.get(page)))
        print ((';').join(pages_id))
        if len(pages_id) != 0: 
            df_stamps.at [i, 'dcterms:isPartOf'] = ((';').join(pages_id)) 
        else:
            df_stamps = df_stamps.drop(i)
            print ("delete")  

    print (df_stamps)    
    df_stamps.to_csv(os.path.splitext(os.path.basename(csvFile))[0]+"_outputStamps.csv", index=False) #to write df into new file

def editBlocks(csvFile):
    #open and read csv file  "user_id": int,
    dataTypes={"o:is_public":"string","o:owner":"string","dcterms:resource_class":"string","dcterms:isFormatOf":"string","dcterms:number" :"string" ,"o:resource_template":"string","o:item_set":"string","dcterms:title":"string","dcterms:description":"string","dcterms:issued": "string","dcterms:language": "string","dcterms:coverage" :"string","dcterms:type" : "string","dcterms:isPartOf":"string","dcterms:identifier" :"string","media:link":"string","dcterms:references":"string"}                                                                                                                                                                                                                                                                                                                
    df = pandas.read_csv(csvFile, dtype=dataTypes, usecols=dataTypes.keys() , encoding='utf-8')
    df = df[df ["o:resource_template"] != 'Stamp']
    # reset indexes of rows
    df = df.reset_index(drop=True)
    print (df)
    #build dict pages- id
    data = omekaApiItems(id_by_itemSet.get("Pages"))
    dict_id_pageIdentifier = extract_dict_id_pageIdentifier(data)
    print (dict_id_pageIdentifier)
    
    #build dict bible_ref- id
    data = omekaApiItems(id_by_itemSet.get("Stamps"))
    dict_Oid_by_stampID = extract_dict_id_stampIdentifier(data)
    print (dict_Oid_by_stampID)
    print (len(dict_Oid_by_stampID))
    
    r = len(df.index)
    for i in range(r):
        #df.at[row, col] = val
        print (df.at [i, 'dcterms:isPartOf'])
        pages = (df.at [i, 'dcterms:isPartOf']).split(";")
        pages_id = []
        for page in pages:
            if (id_by_numberField.get(page) != None): #page has omeka id
                pages_id.append(str(id_by_numberField.get(page)))
        print ((';').join(pages_id))
        if len(pages_id) != 0: 
            df.at [i, 'dcterms:isPartOf'] = (';').join(pages_id) 
        
        #print (df.loc[i, 'dcterms:isFormatOf'])
        if not pandas.isnull((df.loc[i, 'dcterms:isFormatOf'])) and id_by_stampID.get(df.at [i, 'dcterms:isFormatOf']) != None:
            print (df.at[i, 'dcterms:isFormatOf'])
            df.at [i, 'dcterms:isFormatOf'] = str(id_by_stampID[df.at [i, 'dcterms:isFormatOf']])
            print (df.at[i, 'dcterms:isFormatOf'])
        if len(pages_id) == 0:
            df = df.drop(i)
            print ("delete")  
    
    print (df) 
    df.to_csv(os.path.splitext(os.path.basename(csvFile))[0]+"_output_blocks.csv", index=False) #to write df into new file

# work is iterative so each iteration choose function to run
if __name__ == "__main__":
    omekaApiItemSets()
    #editRefs(sys.argv[1])
    #editQuotes(sys.argv[1])
    #editStamps(sys.argv[1])
    #editBlocks(sys.argv[1])