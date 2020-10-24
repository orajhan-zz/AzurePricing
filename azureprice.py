import requests
from pymongo import MongoClient

# My MongoDB
uri = 'mongodb+srv://jhan:***********@jhanmongo.i5sp1.mongodb.net/<Database>?retryWrites=true&w=majority'
# Refer to https://api.mongodb.com/python/current/examples/tls.html#troubleshooting-tls-errors
cluster = MongoClient(uri,ssl=True,ssl_cert_reqs='CERT_NONE')

# CosmoDB with MongoDB API
#uri = "mongodb://jhancosmodb:*****==@jhancosmodb.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@jhancosmodb@"
#cluster = MongoClient(uri)


def Get_azure_price_list(azure_url):
    res = requests.get(azure_url)
    prices = res.json()
    return prices

def Cleanup_MongoDB(db, collection):
    print("Deleting db name: {0}, collection name: {1}".format(db.name, collection.name))
    # Clear data in collection
    deletes = collection.delete_many({})
    print("number of documents deleted: {0} ".format(deletes.deleted_count))
    print("{} collection has been cleared!!!".format(collection.name))

if __name__ == "__main__":
    # MongoDB collections
    # public url for Azure pricing
    azure_url = "https://prices.azure.com/api/retail/prices"
    db = cluster.get_database("azureprice")
    collection = db.get_collection("azureServices")
    print("Use db name: {0}, collection name: {1}".format(db.name, collection.name))

    #clean up the current Azure pricing
    Cleanup_MongoDB(db, collection)

    while True:
        # Aazure price list
        if str(azure_url).lower() == "none":
            print("no more uri")
            break
        else:
            azure_price_list = Get_azure_price_list(azure_url)
            # Push documents into MongoDB
            collection.insert_many(azure_price_list["Items"])
            azure_url = azure_price_list["NextPageLink"]
            print("Next URI: {}".format(azure_price_list["NextPageLink"]))
    # Azure has 135,023 SKUs as of now.
    print("{} documents have been update".format(collection.estimated_document_count()))


