import pytest

def pytest_addoption(parser):
    parser.addoption('--host', action='store', default='localhost',
                     help='host of mqtt broker')
    parser.addoption('--port', action='store', default='1883',
                     help='port of mqtt broker')
