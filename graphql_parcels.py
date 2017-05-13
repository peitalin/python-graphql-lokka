

import urllib
import json
import requests
import time
from async_promises import Promise


class GraphQLClient:
    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.token = None

    def inject_token(self, token):
        self.token = token

    def query(self, query, variables=None):
        return self._send(query, variables)

    def mutate(self, query, variables=None):
        if not query.strip().startswith('mutation'):
            query = 'mutation ' + query
        return self._send(query, variables)

    def _send(self, query, variables):
        data = {
            'query': query,
            'variables': variables
        }
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

        if self.token is not None:
            headers['Authorization'] = 'Bearer {}'.format(self.token)

        try:
            req = requests.post(self.endpoint, data=json.dumps(data), headers=headers)
        except e:
            print(e)
            raise e

        return req


endpoint = 'https://api.graph.cool/simple/v1/'
gql = GraphQLClient(endpoint=endpoint)


############ examples
query = """
{
  allPredictions {
    prediction
  }
}
"""
res = gql.query(query)

mutation = """
{
  createPrediction(prediction: 1000) {
    id
    prediction
  }
}
"""
res2 = gql.mutate(mutation)

mutation = """
{
  createPrediction(prediction: 1000) {
    id
    prediction
  }
}
"""
res3 = gql.mutate(mutation)
###################################### end examples





###########################
with open('./brisbane_parcels_smaller.geojson') as f:
    gdata = json.loads(f.read())

with open('./brisbane_gis_address.geojson') as f:
    addresses = json.loads(f.read())


g = gdata['features'][0]
a = addresses['features'][0]

#####################################


def createGeojsonGeometry(resolve, reject, geojsonGeometry):
    """ Do async stuff here, then pass async result to resolve() """
    #######
    g = geojsonGeometry['geometry']
    createGeojsonGeometryMutation = """
    mutation {{
        createGeojsonGeometry(
            coordinates: "{coordinates}"
            type: "{type}"
        ) {{
            id
        }}
    }}
    """.format(
        coordinates = g['coordinates'],
        type = g['type']
    )
    #####################################################
    try:
        res2 = gql.mutate(createGeojsonGeometryMutation)
        return resolve(res2)
    except:
        return reject(res2)
    #####################################################



def createGeojsonProperties(resolve, reject, geojsonAddress):
    a = geojsonAddress['properties']
    createGeojsonPropertiesMutation = """
    mutation {{
        createGeojsonProperties(
            address: "{address}"
            lot: "{lot}"
            lotPlan: "{lotPlan}"
            plan: "{plan}"
            postcode: "{postcode}"
            state: "{state}"
            streetName: "{streetName}"
            streetNumber: "{streetNumber}"
            streetSuffix: "{streetSuffix}"
            streetType: "{streetType}"
            suburb: "{suburb}"
            unitNumber: "{unitNumber}"
            unitType: "{unitType}"
        ) {{
            id
        }}
    }}
    """.format(
        address =       a['ADDRESS'],
        lot =           a['LOT'],
        lotPlan =       a['LOTPLAN'],
        plan =          a['PLAN'],
        postcode =      a['POSTCODE'],
        state =         a['STATE'],
        streetName =    a['STREET_NAME'],
        streetNumber =  a['STREET_NUMBER'],
        streetSuffix =  a['STREET_SUFFIX'],
        streetType =    a['STREET_TYPE'],
        suburb =        a['CITY_PLACE'].split(',')[0].strip(),
        city =          a['CITY_PLACE'].split(',')[-1].strip(),
        unitNumber =    a['UNIT_NUMBER'],
        unitType =      a['UNIT_TYPE'],
    )
    #####################################################
    try:
        res2 = gql.mutate(createGeojsonPropertiesMutation)
        return resolve(res2)
    except:
        return reject(res2)
    #####################################################



def createGeojson(resolve, reject, results, geojsonAddress):
    res1, res2 = results
    data1 = json.loads(res1.text)
    data2 = json.loads(res2.text)

    # print(data1)
    # print(data2)
    ########
    lngCenter, latCenter = geojsonAddress['geometry']['coordinates']
    mutation = """
    mutation {{
      createGeojson(
        lngCenter: {lngCenter}
        latCenter: {latCenter}
        lotPlan: "{lotPlan}"
        suburbCity: "{suburbCity}"
        geometryId: "{geometryId}"
        propertiesId: "{propertiesId}"
      ) {{
          id
          lngCenter
          latCenter
      }}
    }}
    """.format(
        lngCenter = lngCenter,
        latCenter = latCenter,
        lotPlan = geojsonAddress['properties']['LOTPLAN'],
        suburbCity = geojsonAddress['properties']['CITY_PLACE'],
        geometryId = data1['data']['createGeojsonGeometry']['id'],
        propertiesId = data2['data']['createGeojsonProperties']['id'],
    )
    try:
        res3 = gql.mutate(mutation)
        return resolve(res3)
    except:
        return reject(res3)
    ##############


# createGeometryPromise = Promise(
#     lambda resolve, reject: createGeojsonGeometry(resolve, reject, geojsonGeometry)
# ).then(lambda res: print(res.text))
#
# createPropertiesPromise = Promise(
#     lambda resolve, reject: createGeojsonProperties(resolve, reject, geojsonAddress, geojsonGeometry)
# ).then(lambda res: print(res.text))


promiseAll = Promise.all([
    Promise(lambda resolve, reject: createGeojsonGeometry(resolve, reject, geojsonGeometry)),
    Promise(lambda resolve, reject: createGeojsonProperties(resolve, reject, geojsonAddress)),
]).then(
    lambda results: Promise(lambda resolve, reject: createGeojson(resolve, reject, results, geojsonAddress))
).then(lambda res: print(res.text))


def match_then_post_to_graphql(geojsonAddress):
    try:
        geojsonGeometry = list(filter(lambda x: x['properties']['LOTPLAN'] == geojsonAddress['properties']['LOTPLAN'], gdat['features']))[0]
        return Promise.all([
            Promise(lambda resolve, reject: createGeojsonGeometry(resolve, reject, geojsonGeometry)),
            Promise(lambda resolve, reject: createGeojsonProperties(resolve, reject, geojsonAddress)),
        ]).then(
            lambda results: Promise(lambda resolve, reject: createGeojson(resolve, reject, results, geojsonAddress))
        ).then(lambda res: print(res.text))
    except IndexError as Err:
        print('No match found for ' + geojsonAddress['properties']['ADDRESS'])
        print(Err)


list(map(match_then_post_to_graphql, address['features'][:10]))


for a in address['features'][50:1000]:
    try:
        g = list(filter(lambda x: x['properties']['LOTPLAN'] == a['properties']['LOTPLAN'], gdat['features']))[0]
    except:
        print('NO MATCH:', a['properties']['ADDRESS'])
        continue

    geojsonAddress = a
    geojsonGeometry = g
    print(a['properties']['ADDRESS'])

    promiseAll = Promise.all([
        Promise(lambda resolve, reject: createGeojsonGeometry(resolve, reject, geojsonGeometry)),
        Promise(lambda resolve, reject: createGeojsonProperties(resolve, reject, geojsonAddress)),
    ]).then(
        lambda results: Promise(lambda resolve, reject: createGeojson(resolve, reject, results, geojsonAddress))
    ).then(lambda res: print(res.text))




if false:
    #######
    len(gdat['features'])
    # 575226
    len(address['features'])
    # 602593

    #### only 2 missing matches
    for a in address['features'][:50]:
        try:
            g = list(filter(lambda x: x['properties']['LOTPLAN'] == a['properties']['LOTPLAN'], gdat['features']))[0]
        except:
            print('NO MATCH:', a['properties']['ADDRESS'])
            continue

    #### 26 missing matches
    for g in gdat['features'][:50]:
        try:
            a = list(filter(lambda x: x['properties']['LOTPLAN'] == g['properties']['LOTPLAN'], address['features']))[0]
        except:
            print('NO MATCH:', g['properties']['LOTPLAN'])
            continue

