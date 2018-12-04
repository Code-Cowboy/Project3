import sqlite3
import csv
import json
import numpy as np
import pandas as pd
# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'

pd.set_option("max_colwidth",16)
pd.set_option("precision",1)
pd.set_option('expand_frame_repr', True)
pd.set_option('display.max_columns',10)
try:
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
        
except:
    print("The database could not be created")

statement = '''
        DROP TABLE IF EXISTS 'Bars';
        '''
cur.execute(statement)

statement = '''
        DROP TABLE IF EXISTS 'Countries';
        '''
cur.execute(statement)

statement = '''
        DROP TABLE IF EXISTS 'Bars_old';
        '''
cur.execute(statement)

statement = '''
        CREATE TABLE 'Bars' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Company' TEXT NOT NULL,
                'SpecificBeanBarName' TEXT NOT NULL,
                'REF' TEXT NOT NULL,
                'ReviewDate' TEXT NOT NULL,
                'CocoaPercent' REAL NOT NULL,
                'CompanyLocation' TEXT NOT NULL,
                'Rating' REAL NOT NULL,
                'BeanType' TEXT,
                'BroadBeanOrigin' TEXT NOT NULL,
                'CompanyLocationId' INTEGER,
                'BroadBeanOriginId' INTEGER,
                'CompanyLocationInCode' TEXT,
                'OriginInCode' TEXT,
                'SellRegion' TEXT,
                'SourceRegion' TEXT
                );
        '''
cur.execute(statement)

statement = '''
        CREATE TABLE 'Countries' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Alpha2' VARCHAR(2),
                'Alpha3' VARCHAR(3),
                'EnglishName' TEXT,
                'Region' TEXT,
                'Subregion' TEXT,
                'Population' INTEGER,
                'Area' REAL
                );
        '''

cur.execute(statement)

with open(BARSCSV) as f:
    csvReader = csv.reader(f)
    next(csvReader)
    statement = '''INSERT INTO 'Bars' (Company,SpecificBeanBarName,REF,ReviewDate,CocoaPercent,CompanyLocation,Rating,BeanType,BroadBeanOrigin) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''' 
    for row in csvReader:
        cur.execute(statement,tuple(row))

with open(COUNTRIESJSON,encoding = 'utf-8') as f:
    countries = json.load(f)
    statement = '''INSERT INTO 'Countries'(Alpha2,Alpha3,EnglishName,Region,Subregion,Population,Area) VALUES (?, ?, ?, ?, ?, ?, ?)'''
    for country in countries:
        cur.execute(statement,(country['alpha2Code'],country['alpha3Code'],country['name'],country['region'],country['subregion'],country['population'],country['area']))

statement = '''
        UPDATE Bars
        SET CompanyLocationId = (SELECT Id FROM Countries AS c WHERE CompanyLocation = c. EnglishName);
        '''
        
cur.execute(statement)

statement = '''
        UPDATE Bars
        SET CompanyLocationInCode = (SELECT Alpha2 FROM Countries AS c WHERE CompanyLocation = c. EnglishName);
        '''
cur.execute(statement)

statement = '''
        UPDATE Bars
        SET BroadBeanOriginId = (SELECT Id FROM Countries AS c WHERE BroadBeanOrigin = c. EnglishName);
        '''
cur.execute(statement)
        
statement = '''
        UPDATE Bars
        SET OriginInCode = (SELECT Alpha2 FROM Countries AS c WHERE BroadBeanOrigin = c. EnglishName);
        '''
cur.execute(statement)

statement = '''
        UPDATE Bars
        SET SellRegion = (SELECT Region FROM Countries AS c WHERE CompanyLocation = c. EnglishName);
        '''
cur.execute(statement)

statement ='''
        UPDATE Bars
        SET SourceRegion = (SELECT Region FROM Countries AS c WHERE BroadBeanOrigin = c. EnglishName);
        '''
cur.execute(statement)

statement = '''
        ALTER TABLE Bars
        RENAME TO Bars_old;
        '''
cur.execute(statement)

statement = '''
        CREATE TABLE 'Bars' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Company' TEXT NOT NULL,
                'SpecificBeanBarName' TEXT NOT NULL,
                'REF' TEXT NOT NULL,
                'ReviewDate' TEXT NOT NULL,
                'CocoaPercent' REAL NOT NULL,
                'CompanyLocationId' INTEGER NOT NULL,
                'Rating' REAL NOT NULL,
                'BeanType' TEXT,
                'BroadBeanOriginId' INTEGER
                );
        '''
cur.execute(statement)

statement = '''
        INSERT INTO 'Bars'(Company,SpecificBeanBarName,REF,ReviewDate,CocoaPercent,CompanyLocationId,Rating,BeanType,BroadBeanOriginId)
        SELECT Company,SpecificBeanBarName,REF,ReviewDate,CocoaPercent,CompanyLocationId,Rating,BeanType,BroadBeanOriginId
        FROM Bars_old
        '''
cur.execute(statement)

conn.commit()
conn.close()
# Part 2: Implement logic to process user commands
def process_command(command):
    commandlist = command.split()
    commandword = commandlist[0]
    commandlist.remove(commandword)

    par_pair = []
    for par in commandlist:
        par_pair.append(par.split("="))
    par_pair_flat = [item for sublist in par_pair for item in sublist]
    
    def indicator(j,a,b):
        if j is True:
            return a
        else:
            return b
        
    def bars(sellcountry = None,sourcecountry = None, sellregion = None, sourceregion = None,orderword = 'Rating', toporbottom = "DESC", limit='10'):
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
        
        
        statement = '''SELECT SpecificBeanBarName, Company, CompanyLocation, Rating, CocoaPercent, BroadBeanOrigin '''
        statement += '''FROM Bars_old '''
        if sellcountry == None and sourcecountry == None and sellregion == None and sourceregion == None:
            statement +=''
        else:
            statement +='WHERE '
            parameterlist = [indicator(sellcountry == None,'',"CompanyLocationInCode = '%s'" % sellcountry),
                             indicator(sourcecountry == None,'',"OriginInCode = '%s'" % sourcecountry),
                             indicator(sourceregion == None,'', "SourceRegion = '%s'"% sourceregion),
                             indicator(sellregion == None,'',"SellRegion = '%s'" % sellregion)]
            parameterlistnonull = list(filter(('').__ne__,parameterlist))
            statement += " AND ".join(parameterlistnonull)

        statement += ' ORDER BY CAST(%s AS REAL) %s LIMIT %s' % (orderword,toporbottom,limit)
    
        
            
        cur.execute(statement)
        result_set = []
        for i in range(int(limit)):
            result = cur.fetchone()
            result_set.append(result)
            
        resultdata = pd.DataFrame(result_set)
        print(resultdata)
        return result_set
        
    def companies(country = None,region = None,orderword = "Rating", toporbottom = 'DESC',limit = '10'):
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
        
        if orderword == 'Rating':
            agword = 'AVG(Rating)'
        elif orderword == 'Cocoa':
            agword = 'AVG(CocoaPercent)'
        elif orderword == 'bars_sold':
            agword = 'COUNT(*)'
            
        statement = '''SELECT Company,CompanyLocation, %s ''' % agword
        statement += '''FROM Bars_old '''
        if country == None and region == None:
            statement +=''
        else:    
            statement += '''WHERE '''
            parameterlist = [indicator(country == None,'',"CompanyLocationInCode = '%s'" % country),
                             indicator(region == None,''," SellRegion = '%s'" % region)]
            parameterlistnonull = list(filter(('').__ne__,parameterlist))
            statement += " AND ".join(parameterlistnonull)
        
        statement += ''' GROUP BY Company '''
        statement +='''HAVING COUNT(*) > 4 '''
        statement +='''ORDER BY %s %s LIMIT %s''' % (agword,toporbottom,limit)
        
        cur.execute(statement)
        result_set = []
        for i in range(int(limit)):
            result = cur.fetchone()
            result_set.append(result)
            
        resultdata = pd.DataFrame(result_set)
        print(resultdata)
        return result_set
            
    def countries(region = None,sellorsource = True,orderword = "Rating", toporbottom = 'DESC',limit = '10'):
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
        
        if orderword == 'Rating':
            agword = 'AVG(Rating)'
        elif orderword == 'Cocoa':
            agword = 'AVG(CocoaPercent)'
        elif orderword == 'bars_sold':
            agword = 'COUNT(*)'
            
        if sellorsource == True:
            keyword1 = 'SellRegion'
            groupword = "CompanyLocation"
        else:
            keyword1 = 'SourceRegion'
            groupword = "BroadBeanOrigin"
        
        statement = '''SELECT %s, %s, %s ''' % (groupword,keyword1,agword)
        statement += '''FROM Bars_old '''
        if region == None:
            statement +=''
        else:    
            statement += '''WHERE '''
            parameterlist = [indicator(region == None,''," %s = '%s'" % (keyword1,region))]
            parameterlistnonull = list(filter(('').__ne__,parameterlist))
            statement += " AND ".join(parameterlistnonull)
        
        statement += ''' GROUP BY %s ''' % groupword
        statement +='''HAVING COUNT(*) > 4 '''
        statement +='''ORDER BY %s %s LIMIT %s''' % (agword,toporbottom,limit)
        
        cur.execute(statement)
        result_set = []
        for i in range(int(limit)):
            result = cur.fetchone()
            result_set.append(result)
            
        resultdata = pd.DataFrame(result_set)
        print(resultdata)
        return result_set
    
    def regions(sellorsource = True,orderword = "Rating", toporbottom = 'DESC',limit = '10'):
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()

        if orderword == 'Rating':
            agword = 'AVG(Rating)'
        elif orderword == 'Cocoa':
            agword = 'AVG(CocoaPercent)'
        else:
            agword = 'COUNT(*)'
            
        if sellorsource == True:
            keyword1 = 'SellRegion'
            
        else:
            keyword1 = 'SourceRegion'
        
        statement = '''SELECT %s, %s ''' % (keyword1,agword)
        statement += '''FROM Bars_old '''
        statement +='''WHERE %s IS NOT NULL''' % keyword1
        statement += ''' GROUP BY %s ''' % keyword1
        statement +='''HAVING COUNT(*) > 4 '''
        statement +='''ORDER BY %s %s LIMIT %s''' % (agword,toporbottom,limit)
        
        cur.execute(statement)
        result_set = []
        i = 0
        while 1:
            i +=1
            result = cur.fetchone()
            if result is None or i>int(limit):
                break
            else:
                result_set.append(result)
        resultdata = pd.DataFrame(result_set)
        print(resultdata)
        return result_set
    
    if commandword == 'bars':
        validcommand = ['sellcountry','sellregion','sourcecountry','sourceregion','top','bottom','ratings','cocoa']
        for par in par_pair:
            if not par[0] in validcommand:
                print("Command not recognized: %s" % command)
                return 0
        if "sellcountry" in par_pair_flat:
            sellcountryw = par_pair_flat[par_pair_flat.index("sellcountry")+1]
        else:
            sellcountryw = None
        if "sellregion" in par_pair_flat:
            sellregionw = par_pair_flat[par_pair_flat.index("sellregion")+1]
        else:
            sellregionw = None
        if "sourcecountry" in par_pair_flat:
            sourcecountryw = par_pair_flat[par_pair_flat.index("sourcecountry")+1]
        else:
            sourcecountryw = None
        if "sourceregion" in par_pair_flat:
            sourceregionw = par_pair_flat[par_pair_flat.index("sourceregion")+1]
        else:
            sourceregionw = None
        if 'ratings' in par_pair_flat:
            orderwordw = 'Rating'
        elif 'cocoa' in par_pair_flat:
            orderwordw = 'CocoaPercent'
        else:
            orderwordw = 'Rating'
        if 'bottom' in par_pair_flat:
            toporbottomw = "ASC"
            limitw = par_pair_flat[par_pair_flat.index("bottom")+1]
        elif 'top' in par_pair_flat:
            toporbottomw = "DESC"
            limitw = par_pair_flat[par_pair_flat.index("top")+1]
        else:
            toporbottomw = "DESC"
            limitw = 10
        return bars(sellcountry = sellcountryw, sellregion = sellregionw, 
                    sourcecountry = sourcecountryw, sourceregion = sourceregionw,orderword= orderwordw,
                    toporbottom = toporbottomw, limit = limitw)
    elif commandword == 'companies':
        validcommand = ['country','region','top','bottom','ratings','bars_sold','cocoa']
        for par in par_pair:
            if not par[0] in validcommand:
                print("Command not recognized: %s" % command)
                return 0
        if "country" in par_pair_flat:
            countryw = par_pair_flat[par_pair_flat.index("country")+1]
        else:
            countryw = None
        if "region" in par_pair_flat:
            regionw = par_pair_flat[par_pair_flat.index("region")+1]
        else:
            regionw = None
        if 'ratings' in par_pair_flat:
            orderwordw = 'Rating'
        elif 'cocoa' in par_pair_flat:
            orderwordw = 'Cocoa'
        elif 'bars_sold' in par_pair_flat:
            orderwordw = 'bars_sold'
        else:
            orderwordw = 'Rating'
        if 'bottom' in par_pair_flat:
            toporbottomw = "ASC"
            limitw = par_pair_flat[par_pair_flat.index("bottom")+1]
        elif 'top' in par_pair_flat:
            toporbottomw = "DESC"
            limitw = par_pair_flat[par_pair_flat.index("top")+1]
        else:
            toporbottomw = "DESC"
            limitw = 10
        return companies(country = countryw, region = regionw, 
                    orderword= orderwordw,
                    toporbottom = toporbottomw, limit = limitw)
    elif commandword == 'countries':
        validcommand = ['region','ratings','cocoa','bars_sold','sellers','sources','top','bottom']
        for par in par_pair:
            if not par[0] in validcommand:
                print("Command not recognized: %s" % command)
                return 0
        if 'region' in par_pair_flat:
            regionw = par_pair_flat[par_pair_flat.index("region")+1]
        else:
            regionw = None
        if 'sources' in par_pair_flat:
            sellorsourcew = False
        else:
            sellorsourcew = True
        if 'ratings' in par_pair_flat:
            orderwordw = 'Rating'
        elif 'cocoa' in par_pair_flat:
            orderwordw = 'Cocoa'
        elif 'bars_sold' in par_pair_flat:
            orderwordw = 'bars_sold'
        else:
            orderwordw = 'Rating'
        if 'bottom' in par_pair_flat:
            toporbottomw = "ASC"
            limitw = par_pair_flat[par_pair_flat.index("bottom")+1]
        elif 'top' in par_pair_flat:
            toporbottomw = "DESC"
            limitw = par_pair_flat[par_pair_flat.index("top")+1]
        else:
            toporbottomw = "DESC"
            limitw = 10
        return countries(region = regionw, sellorsource = sellorsourcew,
                    orderword= orderwordw,
                    toporbottom = toporbottomw, limit = limitw)

    elif commandword == 'regions':
        validcommand = ['sellers','sources','ratings','cocoa','bars_sold','top','bottom']
        for par in par_pair:
            if not par[0] in validcommand:
                print("Command not recognized: %s" % command)
                return 0
        if 'sources' in par_pair_flat:
            sellorsourcew = False
        else:
            sellorsourcew = True
        if 'ratings' in par_pair_flat:
            orderwordw = 'Rating'
        elif 'cocoa' in par_pair_flat:
            orderwordw = 'Cocoa'
        elif 'bars_sold' in par_pair_flat:
            orderwordw = 'bars_sold'
        else:
            orderwordw = 'Rating'
        if 'bottom' in par_pair_flat:
            toporbottomw = "ASC"
            limitw = par_pair_flat[par_pair_flat.index("bottom")+1]
        elif 'top' in par_pair_flat:
            toporbottomw = "DESC"
            limitw = par_pair_flat[par_pair_flat.index("top")+1]
        else:
            toporbottomw = "DESC"
            limitw = 10
        return regions(sellorsource = sellorsourcew,
                    orderword= orderwordw,
                    toporbottom = toporbottomw, limit = limitw)
    else:
        print("Command not recognized: %s" % command)
    
    conn.close()
def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    
    help_text = load_help_text()
    while 1:
        
        response = input('Enter a command: ')
        if response == 'exit':
            print("bye")
            break
        if response == 'help':
            print(help_text)
            continue
        process_command(response)
# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    interactive_prompt()
