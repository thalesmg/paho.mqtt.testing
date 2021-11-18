import pytest

def pytest_addoption(parser):
    parser.addoption('--host', action='store', default='localhost',
                     help='host of mqtt broker')
    parser.addoption('--port', action='store', default='1883',
                     help='port of mqtt broker')
    parser.addoption('--base-socket-timeout', action='store', type=float,
                     default=0.1, help='base socket timeout amount'
                                       ' for receiving responses.'
                                       ' unit is seconds.')
    parser.addoption('--base-sleep', action='store', type=float,
                     default=0.1, help='base sleep amount to be used '
                                       ' between timing sensitive operations.'
                                       ' unit is seconds')
    parser.addoption('--base-wait-for', action='store', type=float,
                     default=1.0, help='base amount to be used '
                                       ' while waiting for callback responses.'
                                       ' unit is seconds')
