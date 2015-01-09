import pytest
import jobmon.redisbackend as rb


@pytest.fixture
def rback():
    rback = rb.RedisDataStore('localhost', db=1)
    rback.conn.flushdb()
    return rback


def test_basic(rback):
    rback.job_status([])
    rback.job_status(['', '', 'v'])

    rback.worker_status([])
    rback.worker_status(['', '', 'v'])


def test_job_status(rback):
    assert(rback.conn.get('jobs:failed') is None)
    assert(rback.conn.get('jobs:numdone') is None)
    rback.job_succeed()
    rback.job_fail()
    assert('1' == rback.conn.get('jobs:failed'))
    assert('1' == rback.conn.get('jobs:numdone'))

# TODO: Someday write more tests!
