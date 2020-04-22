import json,pysolr

from datetime import datetime

#solr = pysolr.Solr('http://192.168.36.131:8886/solr/geoTweets')
#
#results = solr.search(q='*:*',start='649',rows='10')

# The ``Results`` object stores total results found, by default the top
# ten most relevant results and any additional data like
# facets/highlighting/spelling/etc.
#print(len(results))

# Just loop over it to access the results.
#for result in results:
    #import pdb;pdb.set_trace()
    #print(result)

index=[{"created_at":"2020-04-22T15:58:45Z",
"id":"12529902317180443648",
"text":"I'm really loving the whole not working out thing during this quarantine. #lazyAF #COVID19",
"user_name":"jacek_88",
"city":"Chicago, IL",
"country_code":"US",
"country":"United States",
"latitude":41.644102,
"longitude":-87.940033}]

#index = [{
#        "created_at": str(datetime.strptime(result["created_at"], "%a %b %d %H:%M:%S %z %Y").strftime("%Y-%m-%dT%H:%M:%SZ")),
#        "id": result["id"],
#        "text": result["text"],
#        "user_name": result["user_name"],
#        "longitude":(result["place"]["bounding_box"]["coordinates"][0][0][0]+result["place"]["bounding_box"]["coordinates"][0][2][0])/2, 
#        "latitude":(result["place"]["bounding_box"]["coordinates"][0][0][1]+result["place"]["bounding_box"]["coordinates"][0][1][1])/2,
#        "city":result["city"],
#        "country_code":result["country_code"],
#        "country":result["country"]
#    }]
solr2 = pysolr.Solr('http://192.168.36.131:8886/solr/geoBasedTweets')
solr2.add(index, commit=True)
solr2.commit()