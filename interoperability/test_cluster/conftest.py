import pytest

def pytest_addoption(parser):
    parser.addoption('--host1', action='store', default='localhost',
                     help='host of mqtt broker')
    parser.addoption('--port1', action='store', default='1883',
                     help='port of mqtt broker')
    
    parser.addoption('--host2', action='store', default='localhost',
                     help='host of mqtt broker')
    parser.addoption('--port2', action='store', default='1883',
                     help='port of mqtt broker')
