

# Graphql Client for Python

This is a simple wrapper to help upload data to a Graphql endpoint.

the GraphQLClient class has two methods: query and mutate.

## What problem this solves for me

Javascript is not the best when it comes to handling data.
Javscript can't load objects larger than ~256mb so large datasets can't be easily processed in memory without special lazy-loading libraries.

Python, and the pandas library is much better at this.

### Workflow
1) Process geojson data in python.
2) write a Graphql mutation
3) execute promise, using the async_promise library.




### QGIS
Parcel data is from: DP_PROP_LOCATION_INDEXQLD






