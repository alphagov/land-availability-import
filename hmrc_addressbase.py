'''
Setup:

pip install requests_cache
export HMRC_USER=<username>
export HMRC_PASSWORD=<password>
'''
import os
import time

import requests_cache

requests_cache.install_cache('hmrc_api')


def lookup_postcode_in_addressbase(postcode):
    url = 'https://txm-al-demo.tax.service.gov.uk/v2/uk/addresses'
    # params e.g. ?postcode=CB21NS'
    params = dict(postcode=str(postcode.replace(' ', '')))
    return call_hmrc(url, params)

def lookup_uprn_in_addressbase(uprn):
    url = 'https://txm-al-demo.tax.service.gov.uk/v2/uk/addresses'
    # params e.g. ?uprn=200003273799'
    params = dict(uprn=int(uprn))
    return call_hmrc(url, params)

def call_hmrc(url, params):
    headers = {'user-agent': 'gds-import'}
    auth = (os.environ['HMRC_USER'], os.environ['HMRC_PASSWORD'])
    requests_session = requests_cache.CachedSession('hmrc_api')
    requests_session.hooks = {'response': make_requests_throttle_hook(1.0)}
    response = requests_session.get(
        url, params=params, auth=auth, headers=headers)
    response.raise_for_status()
    return response.json()

def make_requests_throttle_hook(timeout=1.0):
    def hook(response, *args, **kwargs):
        if not getattr(response, 'from_cache', False):
            print('sleeping')
            time.sleep(timeout)
        return response
    return hook

def serialize_address(address):
    '''Turn the address from AddressBase into a single string.
    e.g. {'lines': ['10 Portal Road'],
          'town': 'Stafford',
          'county': 'Staffordshire',
          'postcode': 'ST16 3QR',
          'subdivision': {'code': 'GB-ENG', 'name': 'England'}
         }
    '''
    all_lines = []
    for key, value in address.items():
        if key == 'lines':
            all_lines.extend(value)
        elif key in ('subdivision', 'country'):
            continue
        else:
            all_lines.append(value)
    return ', '.join(all_lines)
