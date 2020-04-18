import json,pysolr

from datetime import datetime

solr = pysolr.Solr('http://192.168.36.131:8886/solr/geoTweets')

results = solr.search(q='*:*',start='6484',rows='100000')

# The ``Results`` object stores total results found, by default the top
# ten most relevant results and any additional data like
# facets/highlighting/spelling/etc.
print(len(results))
# Just loop over it to access the results.
for result in results:
    #import pdb;pdb.set_trace()
    print(result)
    index = [{
            "created_at": str(result['created_at']),
            "id": result["id"],
            "text": result["text"],
            "user_name": result["user_name"],
            "longitude":result["longitude"],
            "latitude":result["latitude"],
            "city":result["city"],
            "country_code":result["country_code"],
            "country":result["country"]

        }]
    solr2 = pysolr.Solr('http://192.168.36.131:8886/solr/geoBasedTweets')
    solr2.add(index, commit=True)
    solr2.commit()