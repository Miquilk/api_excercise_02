import requests
import pandas as pd
import numpy as np
import myKey
import glob, os, os.path
from os import listdir
import time

def makingRequest(varA, varB, key):
    urlApi = f"https://api.nytimes.com/svc/books/v3/{varA}{varB}?api-key={key}"
    r = requests.get(urlApi)
    s = r.json()
    return s

def getCategories(key): #This function make a request of all categorys and save in a list    
    s = makingRequest('lists', '/names.json', key)
    categoryListTemp = list(map(lambda category: category['list_name'], s['results']))
    return categoryListTemp

def parseToDict(responsesList): # This function transform the list into a dictionary
    responseDict = {
        'category': responsesList
    }
    return responseDict
	
def saveFromDictToCsv(data:dict, fileName:str):#This function save the dictionary in a CSV file
    df = pd.DataFrame.from_dict(data)
    df.to_csv(f'{fileName}.csv')

def getParseSaveCategorysCsv():
    passWord = myKey.getKey()
    category = getCategories(passWord) #This function make a request of all categorys and save in a list    
    tempDict = parseToDict(category) # This function transform the list into a dictionary
    saveFromDictToCsv(tempDict, 'categoryList') #This function save the dictionary in a CSV file

def getParseSaveBooksCsv(): 
    passWord = myKey.getKey()
    fromCsvDf = readCategoryDF()
    categoryList = convertCategorysDfToList(fromCsvDf)
    saveBooksByCategory(categoryList, passWord)

def readCategoryDF():
    category = pd.read_csv('categoryList.csv')
    del category['Unnamed: 0']
    return category
	
def convertCategorysDfToList(categoryList):
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
	
def saveFromlistToCsv(data:list, fileName:str):
    df = pd.DataFrame(data)
    df.to_csv(f'database/{fileName}.csv')


def getNameOfCsvFiles():
    all_files = listdir("database")    
    csv_files = list(filter(lambda f: f.endswith('.csv'), all_files))   
    return csv_files


def creatingDfsDictionary(csvFileList):
    dictionary = {name: pd.read_csv(f'database/{name}') for name in csvFileList}
    dictionary = deleteColumnOnDict(dictionary, 'Unnamed: 0')
    dictionary = addCategoryNameColumn(dictionary)
    return dictionary


def deleteColumnOnDict(dictionary, column):
    for category in dictionary:
        del dictionary[category][column]
    return dictionary


def addCategoryNameColumn(dictionary):
    for category in dictionary:
        dictionary[category]['Category'] = (f'{category[:-4]}')
    return dictionary


def unifyDfsWithDict(dict):
    frame = [dict[category] for category in dict]
    unifyDfs = pd.concat(frame)
    unifyDfs.columns = ['Rank', 'Rank in Last Week', 'Weeks on list', 'ISBN', 'Publisher', 'Description', 'Title', 'Author', 'Category']
    unifyDfs = unifyDfs[['Rank', 'Title', 'Author', 'Category', 'Publisher', 'Rank in Last Week', 'Weeks on list', 'ISBN', 'Description' ]]
    unifyDfs = unifyDfs[['Rank', 'Title', 'Author', 'Category', 'Publisher', 'Rank in Last Week', 'Weeks on list', 'ISBN', 'Description' ]]
    unifyDfs["Author Surname"] = unifyDfs["Author"].str.split(" ").str.get(1)
    unifyDfs = changeColumnPosition('Author Surname', 3, unifyDfs)
    unifyDfs.fillna(value=np.nan, inplace=True)
    return unifyDfs


def changeColumnPosition(column = str, columnPosition = int, Df = pd.DataFrame):
    columnName = Df.pop(column)
    Df.insert(columnPosition, column, columnName)
    return Df


def howManyCategorys():
    dataFrame = creatingGeralDF()
    count = dataFrame['Category'].nunique()
    return print (f'This data frame has {count} categories.')


def numOfBooksByCategory():
    dataFrame = creatingGeralDF()
    authorList = dataFrame['Category'].unique()
    dictOfCounters = {}
    for category in authorList:        
        totalBooksInCategory = dataFrame.query(f"Category == '{category}'")
        bookCounter = totalBooksInCategory['Title'].nunique()
        dictOfCounters[category] = bookCounter
    counterDF = pd.DataFrame.from_dict(dictOfCounters, orient='index')
    counterDF.columns =['Num. of Books']
    counterDF = counterDF.sort_values('Num. of Books', ascending=False)
    return counterDF


def repeatedAuthor():
    dataFrame = creatingGeralDF()
    dataFrame['Author'] = dataFrame['Author'].str.replace("'", "")
    df = dataFrame.groupby(by =['Author'])['Category'].count()
    df = df.reset_index()
    df.columns=['Author', 'Number of Categories']
    df = df.sort_values('Number of Categories', ascending=False).reset_index()
    del df['index']
    return df


def countBooksPerCategory(): #ok
    dataFrame = creatingGeralDF()
    df = dataFrame[['Title', 'Author', 'Category']]
    df = df.groupby(by =['Title', 'Author'])['Category'].count()
    df = df.reset_index() 
    df = df.query(f"Category > 1")
    df = df.sort_values('Category', ascending=False)
    df = df.reset_index()
    df = df.rename(columns = {'Category':'Num. Categories'})
    del df['index']
    pd.set_option('display.expand_frame_repr', False)
    return df


def countAutorBooksInCategory(category = str):
    csvFilesName = getNameOfCsvFiles()
    allBooksDictionary = creatingDfsDictionary(csvFilesName)
    tempDf = allBooksDictionary[f'{category}.csv']['author'].value_counts()
    df = pd.DataFrame()
    df = pd.concat([df, tempDf])
    df = df.reset_index()
    df.columns = ['Author', f'Num. of times in category: {category}']
    df[f'Num. of times in category: {category}'] = df[f'Num. of times in category: {category}'].astype(int)
    pd.set_option('display.expand_frame_repr', False) 
    return df


def publishersRanking():
    dataFrame = creatingGeralDF()
    pubList = dataFrame['Publisher'].unique()
    dictOfPublisher = {}
    for publi in pubList:     
        totalPubli = dataFrame.query(f'Publisher == "{publi}"')
        publiCounter = totalPubli['Title'].nunique()
        dictOfPublisher[publi] = publiCounter
    counterDF = pd.DataFrame.from_dict(dictOfPublisher, orient='index')
    counterDF.columns =['Num. of publish']
    counterDF = counterDF.sort_values('Num. of publish', ascending=False)
    pd.set_option('display.max_rows', None)
    return counterDF   


def mostPresentPublisher():
    csvFilesName = getNameOfCsvFiles()
    dictionary = creatingDfsDictionary(csvFilesName)
    dataFrame = creatingGeralDF()
    mostFrequentdict = {}
    for category in dictionary:
        tempDf = dataFrame.query(f'Category == "{category[:-4]}"')
        frequentPublisher = tempDf['Publisher'].mode()
        if len(frequentPublisher) > 1:
            mostFrequentdict[f'{category[:-4]}'] = 'All publishers in this category only appear once'
        else: 
            mostFrequentdict[f'{category[:-4]}'] = frequentPublisher.to_string(index=False) 
    frame = pd.DataFrame.from_dict(mostFrequentdict, orient='index')
    frame.columns = ['Most popular publisher']
    return frame


def deleteExpecificColumnDf(column, DataFrame):
    del DataFrame[column]
    del DataFrame['index']
    return DataFrame


def creatingGeralDF():
    #This function create the geral DataFrame with the formated columns names
    csvFilesName = getNameOfCsvFiles()
    allBooksDictionary = creatingDfsDictionary(csvFilesName)
    unifyDfs = unifyDfsWithDict(allBooksDictionary)
    unifyDfs = unifyDfs.reset_index()
    return unifyDfs


def showAllBooksWithISBN():
    geralDf = creatingGeralDF()
    geralDf = geralDf[['Title','ISBN']].set_index('Title').dropna()
    pd.set_option('display.max_rows', None)
    return geralDf


def getDescriptionByISBN(isbn):
    geralDf = creatingGeralDF()
    byIsbnDf = geralDf.loc[geralDf["ISBN"] == isbn].dropna()
    stringExtract = byIsbnDf['Description'].to_string()
    string = stringExtract
    return string

