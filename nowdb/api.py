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
        else:
            return resp.json()

    def select_all(self, **kwargs):
        """
        Operation: select_all
        """
        return self.__post('select_all', kwargs)


if __name__ == '__main__':
    api = NowDBAPI(token='555002ab88d909e8324aaea01',
                   project='python_nowdb',
                   app_id='556af6e3c1f6d04391f004740')
    api.set_collection('sample')

    try:
        result = api.select_all(limit=5)
    except NowDBException as e:
        print e.message
    else:
        print result
