from elasticsearch import Elasticsearch,helpers
from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.connections import connections
import csv,json
import sys
import logging
class Cars:
    def __init__(self):
        try:
            self.es = Elasticsearch(HOST="http://localhost",PORT=9200)
            self.es = Elasticsearch()
        except Exception as e:
            print("Exception while connection with elasticsearch:",e)
    
    def createIndex(self,dataSet):
        """To create a index"""
        try:
            if(self.es.indices.exists(index="car_dataset")):
                print(f"{dataSet} index already exists")
            else:
                self.es.indices.create(index=dataSet,ignore=400)
                if(self.es.indices.exists(index="car_dataset")):
                    print(f"{dataSet} index has been created")
        except Exception as e:
            print("Exception while creating index:",e)

    def deleteIndex(self,indexName):
        """To delete a index"""
        try:
            if(self.es.indices.exists(index=indexName)==False):
                print(f"{indexName} index does not exists")
            else:
                self.es.indices.delete(index=indexName)
                if(self.es.indices.exists(index=indexName)==False):
                    print(f"{indexName} index has been deleted")
                else:
                    print(f"{indexName} index has not been deleted")
        except Exception as e:
            print("Exception while deleting index:",e)

    def loadCSV(self,csvFilePath,indexName):
        """To load csv file into elasticsearch"""
        data={}
        try:
            with open(csvFilePath) as csvFile:
                csvReader = csv.DictReader(csvFile)
                for rows in csvReader: 
                    docId=rows["index"]
                    data=rows
                    jsonData=json.dumps(data)
                    self.es.index(index=indexName,doc_type='cars',id=docId,body=jsonData)
        except OSError:
            print("Could not open/read file")
            sys.exit()
        except Exception as e:
            print("Eception while loading index:",e)

    def searchCarBrand(self,brand):
        """Return all matching Elasticsearch documents.filter on "brand" if provided in querystring otherwise return all """
        try:
            docList=[]
            resDict={}
            res = self.es.search(index="car_dataset", doc_type="cars", body={"query": {"match": {"brand":brand}}})
            print(res.status_code)
            print("%d documents found" % res['hits']['total'])
            docNo=res['hits']['total']
            if docNo>0:
                for doc in res['hits']['hits']:
                    docList.append(doc['_source'])
                resDict['car_dataset']=docList
                resDict['message']="request success"
                resDict['status_code']=200
            else:
                for doc in res['hits']['hits']:
                    docList.append(doc['_source'])
                resDict['car_dataset']=docList
                resDict['message']="request fail"
                resDict['status_code']=404            
            print(resDict)
        except Exception as e:
            print("Eception while getting data of car brand",e)

    def updateCarLot(self,docId,lot):
        """Update the document to include the lot from the user input"""
        resDict={}
        try:
            
            self.es.update(index='car_dataset',doc_type='cars', id=docId,body={"doc": {"lot": lot}})
            res=self.es.get(index='car_dataset',doc_type='cars', id=docId)
            resDict['document']=res
            resDict['message']="request success"
            resDict['status_code']=200
            print(resDict)
        except Exception as e:
            resDict['document']=[]
            resDict['message']="request fail"
            resDict['status_code']=404
            print(resDict)
            

    def deleteDocument(self,docId):
        """Update the document to include the lot from the user input"""
        try:
            resDict={}
            res=self.es.delete(index='car_dataset',doc_type='cars', id=docId)
            resDict['document_id']=res['_id']
            resDict['message']="request success"
            resDict['status_code']=200
            print(resDict)
        except Exception as e:
            resDict['document']=[]
            resDict['message']="request fail"
            resDict['status_code']=404
            print(resDict)

    def aggDocument(self):
        """return aggreagated number of documents by year"""
        aggResult={}
        try:
            res={}
            resList=[]
            # Define a default Elasticsearch client
            client = connections.create_connection(hosts=['http://localhost:9200'])
            s = Search(using=client, index="car_dataset", doc_type="cars")
            s.aggs.bucket('by_year', 'terms', field='year.keyword', size=1)
            s = s.execute()
            aggRes=s.aggregations.by_year.buckets[0]
            for item in s.aggregations.by_year.buckets:
                res['year']=item.key
                res['documents']=item.doc_count
            resList.append(res)
            aggResult['car_dataset']=resList
            aggResult['message']="request success"
            aggResult['status_code']=200
            print(aggResult) 
           
        except Exception as e:
            aggResult['car_dataset']=[]
            aggResult['message']="request fail"
            aggResult['status_code']=404


car=Cars()
# car.createIndex("car_dataset")
# car.loadCSV(csvFilePath='cars_datasets.csv',indexName='car_dataset')
# car.searchCarBrand("toyota")
# car.updateCarLot(docId=10,lot=40)
# car.deleteDocument(docId=12)
car.aggDocument()
# car.deleteIndex("car_dataset")