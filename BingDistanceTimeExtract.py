# -*- coding: utf-8 -*-
"""
Created on Wed Feb  6 13:13:28 2019

@author: iasedric
"""

class BingMapsDTExtract:
    """ Class used to get the travel distances and times between two addresses.
    
    List of methods:
       >> getnewqueries(server,db,query): Extracting new source and destination information from SQL Server
           server   - Required  :  SQL Server name (Str)
           db:      - Required  :  Data Base name (Str)
           query    - Required  : SQL query (Str)
           NB: This methods expects to receive two columns: [Source] and [Destination]
       >>getpastqueries(server,db,query): Extracting past queries to avoid overusing the Bing API
           server   - Required  :  SQL Server name (Str)
           db:      - Required  :  Data Base name (Str)
           query    - Required  : SQL query (Str)
           NB: This methods expects to receive four columns: [KeyID],[Source],[Destination],[TravelDuration] and [TravelDistance]
       >>cleanqueries(): Checking the new and past queries to select the net new ones and the ones that were already queried in the past
       >>extractfrombing(file): Extracting the TravelDuration and TravelTime using BingAPI
           file     - Required  :   path to the file where the BingMapsKey is stored (Str)
       >>storequeries(server, db): Storing the results and errors in SQL
           server   - Required  :  SQL Server name (Str)
           db:      - Required  :  Data Base name (Str)

    List of attributes:
       >>self.donequeries  : queries for which the travel distance and time was calculated. Pandas Dataframe [KeyID],[Source],[Destination],[TravelDuration] and [TravelDistance]
       >>self.errorqueries : queries that resulted in an error message from Bing API. Pandas Dataframe [Source] and [Destination]
       >>self.pastqueries  : queries already done in the past. Pandas Dataframe [KeyID],[Source],[Destination],[TravelDuration] and [TravelDistance]
       """
       
    def __init__(self):
        print("Use __doc__ attribute to get the list of attributes and methods for this class")
        
    def _printprogressbar (self,iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█'):
        """
        Private method. Call in a loop to create terminal progress bar
        @params:
            iteration   - Required  : current iteration (Int)
            total       - Required  : total iterations (Int)
            prefix      - Optional  : prefix string (Str)
            suffix      - Optional  : suffix string (Str)
            decimals    - Optional  : positive number of decimals in percent complete (Int)
            length      - Optional  : character length of bar (Int)
            fill        - Optional  : bar fill character (Str)
        """
        percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + '-' * (length - filledLength)
        print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = '\r')
        # Print New Line on Complete
        if iteration == total: 
            print()

        
    def getnewqueries(self,server,db,query):
        ''' Extracting new source and destination information from SQL Server
        @params:
            server   - Required  :  SQL Server name (Str)
            db:      - Required  :  Data Base name (Str)
            query    - Required  : SQL query (Str)
        NB: This methods expects to receive two columns: [Source] and [Destination]
        '''
        
        import sqlalchemy
        import pandas as pd
        
        self.server = server
        self.db = db
        self.query = query
        
        encoding='utf-8'
        driver = 'SQL+Server'      
        engine = sqlalchemy.create_engine('mssql+pyodbc://{}/{}?driver={}?encoding={}'.format(self.server, self.db, driver, encoding))

        NewQueries = pd.read_sql(self.query,con=engine)
        
        #Creating additional columns: Key by concatenating Source and Destination, TravelDuration and TravelDistance
        self.new = pd.DataFrame({'NewKey': NewQueries['Source'].str.cat(others=NewQueries['Destination'],sep='+').rename("NewKey"),
                                   'NewSource': NewQueries['Source'],
                                   'NewDestination': NewQueries['Destination'],
                                   'NewTravelDuration': 0,
                                   'NewTravelDistance': 0})


    
    def getpastqueries(self,server,db,query):
        ''' Extracting past queries to avoid overusing the Bing API
        @params:
            server   - Required  :  SQL Server name (Str)
            db:      - Required  :  Data Base name (Str)
            query    - Required  : SQL query (Str)
        NB: NB: This methods expects to receive five columns: [KeyID],[Source],[Destination],[TravelDuration] and [TravelDistance]
        '''
        
        import sqlalchemy
        import pandas as pd
         
        self.server = server
        self.db = db
        self.query = query
        
        encoding='utf-8'
        driver = 'SQL+Server'      
        engine = sqlalchemy.create_engine('mssql+pyodbc://{}/{}?driver={}?encoding={}'.format(self.server, self.db, driver, encoding))
       
        self.past = pd.read_sql(self.query,con=engine)


    
    def cleanqueries(self):
        ''' Creating a mask that will select queries never made in the past (that are not in PastQueries table). 
         Using a LEFT merge on the Key created above and selecting the ones with NA (not in PastQueries)
         '''
        
        import pandas as pd
        
        merge = self.new.merge(self.past, how='left', left_on='NewKey', right_on='KeyID')
        self.query_mask=merge['KeyID'].isnull()
        
        # Creating output Panda series that will be filled with the results
        self.key = self.new['NewKey'][self.query_mask]
        self.source = self.new['NewSource'][self.query_mask]
        self.destination = self.new['NewDestination'][self.query_mask]
        self.travelduration = self.new['NewTravelDuration'][self.query_mask]
        self.traveldistance = self.new['NewTravelDistance'][self.query_mask]
        
        #Storing the queries that were already made in the past
        #self.pastqueries = self.new[[not i for i in self.query_mask]]  
        
        self.pastqueries  = pd.DataFrame({'KeyID': self.new['NewKey'],
                          'Source': self.new['NewSource'],
                          'Destination': self.new['NewDestination'],
                          'TravelDuration': self.new['NewTravelDuration'],
                          'TravelDistance': self.new['NewTravelDistance']})[[not i for i in self.query_mask]] 

        

    def extractfrombing(self, file):
        '''Extracting the TravelDuration and TravelTime using BingAPI
        @params:
            file     - Required  :   path to the file where the BingMapsKey is stored (Str)
        '''
        
        import urllib.request
        import json
        import pandas as pd
        
        len_s = len(self.source)
        len_d = len(self.destination)
        
        # Your Bing Maps Key 
        bingMapsKey =  open(file, 'r').read()
        
        #Variables to log indexes of errors
        self.error_indexes = []
        all_indexes = list(range(0,len_s))
        
        self._printprogressbar(0, len_s, prefix = 'Progress:', suffix = 'Complete', length = 50)
        
        #Making sure that there is a destination for each source
        if (len_s == len_d):
            
            warning = ""
            
            for i in range(0,int(len_s/12)+1):

                wp_i = 0;
                routeUrl = "http://dev.virtualearth.net/REST/V1/Routes/Driving?"
                
                indexes = []
                
                # One URL can contain up to 25 waypoints. We use 24 for 12 couples (Source > Destination): Source 1 > Destination 1 > Source 2 > Destination 2 > ... > Destination 12
                # Obviously we are not interested in the Destination n > Source n+1 part. We will ignore it lower in the code.
                while (wp_i<12 and (12*i+wp_i) < len_s):
                
                    #print("Index: " + str(12*i+wp_i))
                    indexes.append(12*i+wp_i)
                    
                    encodedSource = urllib.parse.quote(self.source.iloc[12*i+wp_i], safe='')
                    encodedDest = urllib.parse.quote(self.destination.iloc[12*i+wp_i], safe='')
                    
                    routeUrl = routeUrl + "&wp." +str(2*wp_i)+"="+ encodedSource + "&wp." +str(2*wp_i + 1)+"="+ encodedDest
        
                    wp_i += 1
                
                
                routeUrl = routeUrl + "&key=" + bingMapsKey
                #print(routeUrl)
                
                try:
                    request = urllib.request.Request(routeUrl)
                    response = urllib.request.urlopen(request)
                
                    r = response.read().decode(encoding="utf-8")
                    result = json.loads(r)
                    
                except:
                    # N.B.We don't have a way to to identify which couple(s) from the 12 caused the error so we log all the 12 couples as errors.
                    self.error_indexes += indexes
        
                try:
                    # We are interested in the Source --> Destintation and ignoring the Destination --> Source info. So taking only even indexes.        
                    for j in range(0,2*wp_i,2):
         
                        self.travelduration.iloc[12*i+int(j/2)] = result["resourceSets"][0]["resources"][0]["routeLegs"][j]["travelDuration"]
                        self.traveldistance.iloc[12*i+int(j/2)] = result["resourceSets"][0]["resources"][0]["routeLegs"][j]["travelDistance"]
                        
                except:
                    #result may be empty
                    warning = "Warning. No results received from Bing API"
                    
                self._printprogressbar(12*i+wp_i, len_s, prefix = 'Progress:', suffix = 'Complete', length = 50)
                    
            # Preparing the error mask to select the entries with no errors and log the ones with errors
            error_mask = [i not in self.error_indexes for i in all_indexes]
            
            #Creating the DataFrame containing the couples (Source Destination) for which we got a Travel Duration and Travel Distance
            self.donequeries  = pd.DataFrame({'KeyID': self.key,
                          'Source': self.source,
                          'Destination': self.destination,
                          'TravelDuration': self.travelduration,
                          'TravelDistance': self.traveldistance})[error_mask]
            
            #Creating the Dataframe containing the couples (Source Destination) for which we couldn't not get the Travel Duration and Travel Distance
            self.errorqueries = pd.DataFrame({'Source': self.source,'Destination': self.destination})[[not i for i in error_mask]]
            
            if (len(self.error_indexes) != 0):
                print("The script encountered a problem on the following indexes: " + str(self.error_indexes))
                
            if (len(warning) != 0):
                print(warning)
                        
        else:
            print("Source and destination lists have different lengths")


    def extractfrombing_obo(self, file):
        '''Extracting the TravelDuration and TravelTime using BingAPI one by one (obo)
        @params:
            file     - Required  :   path to the file where the BingMapsKey is stored (Str)
        '''
        
        import urllib.request
        import json
        import pandas as pd
        
        len_s = len(self.source)
        len_d = len(self.destination)
        
        # Your Bing Maps Key 
        bingMapsKey =  open(file, 'r').read()
        
        #Variables to log indexes of errors
        self.error_indexes = []
        all_indexes = list(range(0,len_s))
        
        self._printprogressbar(0, len_s, prefix = 'Progress:', suffix = 'Complete', length = 50)
        
        #Making sure that there is a destination for each source
        if (len_s == len_d):
            
            warning = ""
            
            indexes = []
            
            for i in range(0,len_s):

                routeUrl = "http://dev.virtualearth.net/REST/V1/Routes/Driving?"
                
                indexes.append(i)
                
                encodedSource = urllib.parse.quote(self.source.iloc[i], safe='')
                encodedDest = urllib.parse.quote(self.destination.iloc[i], safe='')
                
                routeUrl = routeUrl + "&wp.0="+ encodedSource + "&wp.1="+ encodedDest + "&key=" + bingMapsKey
                
                try:
                    request = urllib.request.Request(routeUrl)
                    response = urllib.request.urlopen(request)
                
                    r = response.read().decode(encoding="utf-8")
                    result = json.loads(r)
                    
                except:
                    self.error_indexes += indexes
        
                try:
       
                    self.travelduration.iloc[i] = result["resourceSets"][0]["resources"][0]["routeLegs"][j]["travelDuration"]
                    self.traveldistance.iloc[i] = result["resourceSets"][0]["resources"][0]["routeLegs"][j]["travelDistance"]
                        
                except:
                    #result may be empty
                    warning = "Warning. No results received from Bing API"
                    
                self._printprogressbar(i, len_s, prefix = 'Progress:', suffix = 'Complete', length = 50)
                    
            # Preparing the error mask to select the entries with no errors and log the ones with errors
            error_mask = [i not in self.error_indexes for i in all_indexes]
            
            #Creating the DataFrame containing the couples (Source Destination) for which we got a Travel Duration and Travel Distance
            self.donequeries  = pd.DataFrame({'KeyID': self.key,
                          'Source': self.source,
                          'Destination': self.destination,
                          'TravelDuration': self.travelduration,
                          'TravelDistance': self.traveldistance})[error_mask]
            
            #Creating the Dataframe containing the couples (Source Destination) for which we couldn't not get the Travel Duration and Travel Distance
            self.errorqueries = pd.DataFrame({'Source': self.source,'Destination': self.destination})[[not i for i in error_mask]]
            
            if (len(self.error_indexes) != 0):
                print("The script encountered a problem on the following indexes: " + str(self.error_indexes))
                
            if (len(warning) != 0):
                print(warning)
                        
        else:
            print("Source and destination lists have different lengths")
                    
                    
    def storequeries(self, server, db):
        '''Storing the results and errors in SQL
        @params: 
            server   - Required  :  SQL Server name (Str)
            db:      - Required  :  Data Base name (Str)
        '''
        
        import sqlalchemy
        
        self.server = server
        self.db = db
        
        encoding='utf-8'
        driver = 'SQL+Server'
        
        engine = sqlalchemy.create_engine('mssql+pyodbc://{}/{}?driver={}?encoding={}'.format(server, db, driver,encoding))
        
        self.donequeries.to_sql('PastQueries', con=engine, if_exists='append', index=False)
        self.errorqueries.to_sql('Errors', con=engine, if_exists='append', index=False)



def main():

    import time
    
    start = time.time()
    
    x = BingMapsDTExtract()
    
    #print(x.__doc__)
    
    Countries = ['Puerto Rico', 'Peru', 'Tunisia', 'Namibia', 'Greece', 'Croatia', 'Bahrain']

    server = 'IAROLAPTOP\IAROSQLSERVER'
    db = 'IARODB'
    
    for country in Countries:
            
        query ="SELECT [Source],[Destination] FROM [dbo].[ListAccounts] WHERE [Country] = '" + str(country) + "'"
        x.getnewqueries(server, db, query)
     
        query ="SELECT [KeyID],[Source],[Destination],[TravelDuration],[TravelDistance] FROM [dbo].[PastQueries]"
        x.getpastqueries(server, db, query)
    
        x.cleanqueries()
        
        print("\nExtracting Travel distances and Travel times for country: " + str(country))
        x.extractfrombing("BingMapsKey.txt")
        
        x.storequeries(server,db)
        
   
    query ="SELECT [Source],[Destination] FROM [dbo].[Errors]"
    x.getnewqueries(server, db, query)    
    
    print("\nExtracting Travel distances and Travel times for errors")
    x.extractfrombing_obo("BingMapsKey.txt")
    
    x.storequeries(server,db)

    
    end = time.time()-start
    print('It took ' + str(round(end,2)) + ' seconds to execute the script.')
    
    #PastQueries = x.pastqueries
    #DoneQueries = x.donequeries
    #ErrorQueries = x.errorqueries        
    

if __name__ == '__main__':
    
    main()