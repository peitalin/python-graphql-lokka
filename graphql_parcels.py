

import urllib
import json
import requests
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
        if not query.startswith('mutation'):
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




def dispatchAsync(resolve, reject):
    """
    Do async stuff here, then pass async result to resolve()
    reject() for errors
    """
    res = gql.mutate(mutation)
    return resolve(res)

def thenDo(res):
    print(res.text)
    return res

promise = Promise(
    lambda resolve, reject: dispatchAsync(resolve, reject)
).then(lambda res: thenDo(res))


