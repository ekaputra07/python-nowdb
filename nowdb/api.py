import requests
from requests import exceptions


class NowDBException(Exception):
    """
    Base exception class for NowDB.
    """
    pass


class NowDBAPI(object):
    """
    Base API connection class for NowDB.
    Will be used as base for every API operations.
    """
    API_BASE_URL = 'http://io.nowdb.net/operation/'

    def __init__(self, token=None, project=None, app_id=None, collection=None):
        self.params = {
            'token': token,
            'project': project,
            'appid': app_id,
            'collection': collection # can be overrided via set_collection.
        }

    def set_collection(self, collection):
        """
        Use this to switch between collection without creating new NowDBAPI instance.
        """
        self.params.update({'collection': collection})

    def __post(self, operation, params={}):
        """
        The actual function to call to server.
        """
        self.params.update(params)

        try:
            resp = requests.post(self.API_BASE_URL + operation, data = self.params)
        except exceptions.ConnectionError as e:
            raise NowDBException('Connection error')
        except exceptions.Timeout as e:
            raise NowDBException('Request timeout')
        except exceptions.HTTPError as e:
            raise NowDBException('Invalid HTTP response')

        # In case server return invalid JSON, return plain text response.
        try:
            return resp.json()
        except ValueError as e:
            return resp.text

    def insert(self, **kwargs):
        """
        @operation: insert
        @params:
            kwargs: data to insert
        """
        return self.__post('insert', kwargs)

    def select_all(self, **kwargs):
        """
        @operation: select_all
        @params:
            kwargs: limit, offset, order, mixin
        """
        return self.__post('select_all', kwargs)

    def select_id(self, id):
        """
        @operation: select_id
        @params:
            id: String, the object id.
        """
        return self.__post('select_id', {'id': id})

    def select_where(self, **kwargs):
        """
        @operation: select_where
        @params:
            kwargs: attribute 1, attribute 2, offset, order, mixin
        @example: api.select_where(title='hello', offset=0, limit=10, order='title')
        """
        return self.__post('select_where', kwargs)


if __name__ == '__main__':
    api = NowDBAPI(token='55002ab88d909e8324aaea01',
                   project='python_nowdb',
                   app_id='56af6e3c1f6d04391f004740')
    api.set_collection('sample')

    # Test - insert
    print 'Testing: insert =============='
    try:
        result = api.insert(title="Hello NowDB")
    except NowDBException as e:
        print e.message
    else:
        print result
    obj_id = result[0]['id']

    # Test - select_all
    print '\nTesting: select_all =============='
    try:
        result = api.select_all(limit=1)
    except NowDBException as e:
        print e.message
    else:
        print result

    # Test - select_id
    print '\nTesting: select_id =============='
    try:
        result = api.select_id(obj_id)
    except NowDBException as e:
        print e.message
    else:
        print result

    # Test - select_where
    print '\nTesting: select_where =============='
    try:
        result = api.select_where(title='Hello NowDB')
    except NowDBException as e:
        print e.message
    else:
        print result
