import requests
import pandas as pd
import myKey
import time

def makingRequest(varA, varB, key):
    urlApi = f"https://api.nytimes.com/svc/books/v3/{varA}{varB}?api-key={key}"
    r = requests.get(urlApi)
    s = r.json()
    return s


def getCategories(key):
    s = makingRequest('lists', '/names.json', key)
    categoryListTemp = list(map(lambda category: category['list_name'], s['results']))
    return categoryListTemp


def parseToDict(responsesList):
    responseDict = {
        'category': responsesList
    }
    return responseDict

def saveFromDictToCsv(data:dict, fileName:str):
    df = pd.DataFrame.from_dict(data)
    df.to_csv(f'{fileName}.csv')

def saveFromlistToCsv(data:list, fileName:str):
    df = pd.DataFrame(data)
    df.to_csv(f'database/{fileName}.csv')

def getDf():
    category = pd.read_csv('categoryList.csv')
    del category['Unnamed: 0']
    return category

def parseListOfCategorys(categoryList):
    listOfCategorys = categoryList['category'].to_list()
    return listOfCategorys

def saveBooksByCategory(categoryList, passWord):    
    for category in categoryList:
        print (f'Getting category: {category}')
        r = makingRequest('lists/current/', f'{category}.json', passWord)
        booksList = parseBooks(r['results']['books'])
        saveFromlistToCsv(booksList, f'{category}')
        time.sleep(5)             
    return r


def parseBooks(data):
    def getFields(book):        
        bookDict = {'rank': book['rank'], 
                    'rank_last_week': book['rank_last_week'],
                    'weeks_on_list': book['weeks_on_list'],
                    'primary_isbn10': book['primary_isbn10'],
                    'publisher': book['publisher'],
                    'description': book['description'],
                    'title': book['title'],
                    'author': book['author']
                    }
        return bookDict
    parseBookListTemp = list(map(lambda book: getFields(book), data))
    return parseBookListTemp


def stage1 (passWord):    
    category = getCategories(passWord)    
    tempDict = parseToDict(category)
    saveFromDictToCsv(tempDict, 'categoryList')

def stage2 (passWord): 
    csvDf = getDf()
    categoryList = parseListOfCategorys(csvDf)
    saveBooksByCategory(categoryList, passWord)
  

def main():
    passWord = myKey.getKey()
    stage1(passWord)
    stage2(passWord)
         

main()

